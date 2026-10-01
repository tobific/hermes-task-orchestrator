"""Owner-private POSIX storage, with descriptor-anchored path traversal."""

from __future__ import annotations
import os
import stat
import uuid
from pathlib import Path

PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600


class StorageError(RuntimeError):
    """Unsafe or inaccessible plugin-owned storage."""


def _absolute(path):
    p = Path(path)
    if ".." in p.parts:
        raise StorageError("storage traversal is not permitted")
    return Path(os.path.abspath(os.fspath(p)))


def configured_storage_directory(path):
    """Resolve trusted configuration ancestors once, never the owned leaf."""
    target = _absolute(path)
    return target.parent.resolve(strict=False) / target.name


def _flags():
    if not hasattr(os, "O_NOFOLLOW") or not hasattr(os, "O_DIRECTORY"):
        raise StorageError("no-follow directory descriptors unavailable")
    return os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)


def _directory_fd(path, *, create=False, secure=False):
    target = _absolute(path)
    if secure and target == Path(target.anchor):
        raise StorageError("refusing to secure filesystem root")
    flags = os.O_DIRECTORY | _flags()
    search = getattr(os, "O_SEARCH", getattr(os, "O_PATH", os.O_RDONLY))
    # Ancestors need traversal only, not directory-listing permission.
    fd = os.open(target.anchor, search | flags)
    try:
        for index, part in enumerate(target.parts[1:]):
            access = os.O_RDONLY if index == len(target.parts) - 2 else search
            try:
                child = os.open(part, access | flags, dir_fd=fd)
            except FileNotFoundError:
                if not create:
                    raise
                try:
                    os.mkdir(part, PRIVATE_DIRECTORY_MODE, dir_fd=fd)
                except FileExistsError:
                    pass
                child = os.open(part, access | flags, dir_fd=fd)
            os.close(fd)
            fd = child
        if secure:
            st = os.fstat(fd)
            if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
                raise StorageError("storage directory is not owner-controlled")
            os.fchmod(fd, PRIVATE_DIRECTORY_MODE)
        return fd
    except BaseException:
        os.close(fd)
        raise


def ensure_private_directory(path):
    target = _absolute(path)
    fd = _directory_fd(target, create=True, secure=True)
    os.close(fd)
    return target


def _check_regular(fd):
    st = os.fstat(fd)
    if not stat.S_ISREG(st.st_mode) or st.st_uid != os.getuid() or st.st_nlink != 1:
        raise StorageError(
            "storage file must be an owner-controlled single-link regular file"
        )
    os.fchmod(fd, PRIVATE_FILE_MODE)


def _open_at(parent, name, mode, create=False, exclusive=False):
    if mode not in {"r", "r+b", "a+b"}:
        raise ValueError("unsupported private file mode")
    flags = os.O_RDWR | _flags() | getattr(os, "O_NONBLOCK", 0)
    if create:
        flags |= os.O_CREAT
    if exclusive:
        flags |= os.O_EXCL
    if mode == "a+b":
        flags |= os.O_APPEND
    fd = os.open(name, flags, PRIVATE_FILE_MODE, dir_fd=parent)
    try:
        _check_regular(fd)
        return os.fdopen(fd, mode, **({} if "b" in mode else {"encoding": "utf-8"}))
    except BaseException:
        os.close(fd)
        raise


def open_private_file(path, mode="a+b", *, create=True):
    target = _absolute(path)
    parent = _directory_fd(target.parent)
    try:
        return _open_at(parent, target.name, mode, create=create)
    finally:
        os.close(parent)


def _repair_tree_fd(fd, budget, depth=0):
    if depth > 16:
        raise StorageError("private storage directory depth exceeded")
    st = os.fstat(fd)
    if not stat.S_ISDIR(st.st_mode) or st.st_uid != os.getuid():
        raise StorageError("storage directory is not owner-controlled")
    os.fchmod(fd, PRIVATE_DIRECTORY_MODE)
    for name in os.listdir(fd):
        budget[0] -= 1
        if budget[0] < 0:
            raise StorageError("private storage repair budget exceeded")
        try:
            st = os.stat(name, dir_fd=fd, follow_symlinks=False)
            if stat.S_ISDIR(st.st_mode):
                child = os.open(
                    name, os.O_RDONLY | os.O_DIRECTORY | _flags(), dir_fd=fd
                )
                try:
                    _repair_tree_fd(child, budget, depth + 1)
                finally:
                    os.close(child)
            else:
                with _open_at(fd, name, "r+b"):
                    pass
        except FileNotFoundError:
            # A concurrent supervisor may have consumed this obsolete marker.
            continue


def repair_private_tree(path):
    fd = _directory_fd(path, secure=True)
    try:
        _repair_tree_fd(fd, [8192])
    finally:
        os.close(fd)


def repair_scheduler_storage(path):
    """Repair known persisted artifacts only; unrelated root entries stay alone."""
    fd = _directory_fd(path, secure=True)
    try:
        for name in os.listdir(fd):
            if name in {"owners", "drained"}:
                child = os.open(
                    name, os.O_RDONLY | os.O_DIRECTORY | _flags(), dir_fd=fd
                )
                try:
                    _repair_tree_fd(child, [8192])
                finally:
                    os.close(child)
            elif name in {"state.json", "state.lock", "state.json.tmp"} or (
                name.startswith(".state.json.") and name.endswith(".tmp")
            ):
                try:
                    with _open_at(fd, name, "r+b"):
                        pass
                except FileNotFoundError:
                    pass
    finally:
        os.close(fd)


def _validate_target(parent, name):
    try:
        with _open_at(parent, name, "r+b"):
            pass
    except FileNotFoundError:
        pass


def atomic_replace_bytes(path, payload, *, prefix=None):
    target = _absolute(path)
    parent = _directory_fd(target.parent, create=True, secure=True)
    temporary = None
    try:
        _validate_target(parent, target.name)
        # Names are host-generated; no caller prefix can escape this descriptor.
        label = prefix or f".{target.name}."
        if "/" in label or "\\" in label or label in {".", ".."}:
            raise StorageError("invalid temporary file prefix")
        for _ in range(10):
            name = f"{label}{uuid.uuid4().hex}.tmp"
            try:
                handle = _open_at(parent, name, "r+b", create=True, exclusive=True)
                temporary = name
                break
            except FileExistsError:
                continue
        else:
            raise StorageError("private temporary file collision budget exhausted")
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        _validate_target(parent, target.name)
        os.replace(temporary, target.name, src_dir_fd=parent, dst_dir_fd=parent)
        temporary = None
        return target
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary, dir_fd=parent)
            except FileNotFoundError:
                pass
        os.close(parent)
