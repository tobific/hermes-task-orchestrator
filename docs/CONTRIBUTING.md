# Contributing

Thank you for looking at this project.

- **Issues are welcome**: bugs, questions, unclear documentation, ideas. Please do not include keys, tokens or other
  private data.
- **Small pull requests are welcome.** One change per pull request, with a test where it makes sense, and the test
  numbers from [TESTING.md](../TESTING.md) before and after.
- **Keep the seams small.** Changes to `patches/` should keep the rule that Hermes behaves exactly as before when
  nothing registers for a seam. Patch 0008 is the current exception (its cron memory isolation applies as soon as it
  is installed); please do not add more such cases without saying so clearly.
- **Be kind.** Never disparage Hermes or its maintainers; this project builds on their work.

## Maintainers of Hermes

You are welcome to reuse and adapt this under MIT, retaining its required copyright and permission notices. No
additional acknowledgement is requested. If you would like a seam as a pull request against current Hermes, or a
test adapted to your test layout, please open an issue; [UPSTREAMING.md](UPSTREAMING.md) describes how each one could
be proposed.
