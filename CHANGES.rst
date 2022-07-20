Changelog
=========

Version 0.5.12
--------------

- Added Message.mandatory field.


Version 0.5.11
--------------

- Change accepted parameters for rabbitmq.Publisher.publish_messages().


Version 0.5.10
--------------

- Fixed packaging issue


Version 0.5.9
-------------

- Fixed minor importing issues


Version 0.5.8
-------------

- Added a `flask_signalbus.rabbitmq` utility module.


Version 0.5.7
-------------

- Fixed bug in the `atomic` decorator. Before this fix, not all
  database serialization errors resulted in a retry.


Version 0.5.6
-------------

- Minor logging improvements.


Version 0.5.5
-------------

- Minor change in the way the "--reapeat" parameter works.


Version 0.5.4
-------------

- Do not use the depreciated `marshmallow-sqlalchemy.ModelSchema`
  class.


Version 0.5.3
-------------

- Register the "after_configured" SQLAlchemy event handler in the
  SignalBusMixin's constructor. This guarantees that the handler will
  be executed even if the event is triggered before init_app().


Version 0.5.2
-------------

- Do not wait after the first DB serialization failure
- Pinned `pytest`, `pytest-cov`, `mock` versions
- Improved docs


Version 0.5.1
-------------

- Changed a message loglevel from "warning" to "info".


Version 0.5.0
-------------

- Added support for `send_signalbus_messages` class method. This can
  greatly improve performance by receiving acknowledges for a whole
  batch of messages at once.
- Added `flushordered()` method, and `flushordered` CLI command. This
  allows sending messages in predictable order, and when auto-flushing
  is disabled -- the correct order is guaranteed.
- General code improvements.
- Improved documentation.


Version 0.4.4
-------------

- Add `models` parameter to flushmany().
- Do not order by primary key if `signalbus_order_by` is not set.


Version 0.4.3
-------------

- Improved performace.
- Added support for `signalbus_order_by` attribute on signals.


Version 0.4.2
-------------

- Added `*options` parameter to `_ModelUtilitiesMixin.get_instance()`
  and `_ModelUtilitiesMixin.lock_instance()`.


Version 0.4.1
-------------

- In CLI commands, log errors instead of printing them


Version 0.4.0
-------------

- Use `marshmallow_sqlalchemy` for signal serialization
- Auto define __marshmallow__ attribute on all models
- Auto define __marshmallow_schema__ attribute on signal models


Version 0.3.7
-------------

- Split implementation into two files: `signalbus.py` and  `utils.py`
- Renamed `cli.py` to `signalbus_cli.py`
- Added `.circleci` directory
- Added `atomic.py` module
- Added new tests


Version 0.3.6
-------------

- Fixed regression, added test


Version 0.3.5
-------------

- Make the undocumented `retry_on_deadlock` function more useful.
- Add new tests


Version 0.3.4
-------------

- Fixed leaking signal sessions bug.


Version 0.3.3
-------------

- Change author's email address


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

- Improved docs


Version 0.3.1
-------------

- Fixed bug in 'flush' CLI command


Version 0.3
-----------

- More efficient flushing algorithm, less prone to DB serialization problems


Version 0.2
-----------

- Initial public release
