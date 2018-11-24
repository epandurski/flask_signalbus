def test_flush_empty(app, signalbus):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0'])
    assert not result.output


def test_flush_pending(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0'])
    assert '1' in result.output
    assert 'processed' in result.output


def test_flush_pending_explicit(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0', 'Signal'])
    assert '1' in result.output
    assert 'processed' in result.output


def test_flush_pending_explicit_wrong_name(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0', 'WrongSignalName'])
    assert not ('1' in result.output and 'processed' in result.output)
    assert 'Warning' in result.output


def test_flush_pending_exclude(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0', '--exclude', 'Signal'])
    assert not result.output


def test_flush_pending_exclude_wrong_name(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0', '--exclude', 'WrongSignalName'])
    assert 'Warning' in result.output
    assert '1' in result.output
    assert 'processed' in result.output


def test_flush_error(app, signalbus_with_pending_error):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush', '--wait', '0'])
    assert isinstance(result.exception, ValueError)


def test_flushmany_nosignals(app, signalbus):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flushmany'])
    assert not result.output


def test_flushmany_empty(app, signalbus, Signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flushmany'])
    assert not result.output


def test_flushmany_pending(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flushmany'])
    assert '1' in result.output
    assert 'processed' in result.output


def test_show_signals(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'signals'])
    assert 'Signal' == result.output.strip()


def test_show_pending(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'pending'])
    assert 'Signal' in result.output
    assert 'Total pending: 1' in result.output
