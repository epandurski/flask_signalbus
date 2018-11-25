Changelog
=========

Version 0.3.2
-------------

- Added `SignalBus.flushmany` method and `flushmany` CLI command.

- Added `wait` optional argument to the `SignalBus.flush` method and
  the `flush` CLI command.

- Added optional `signalbus_autoflush` attribute to signal models,
  which defaults to `True`.

- Fixed a bug caused by not clearing the set of signals added to the
  session on rollback.

- Change `flush` and `flushmany` to obtain row lock before sending the
  message, thus avoiding some rare concurrency issues.


Version 0.3.1
-------------

- Fixed bug in 'flush' CLI command


Version 0.3
-----------

- More efficient flushing algorithm, less prone to DB serialization problems


Version 0.2
-----------

- Initial public release
