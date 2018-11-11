def test_flush_empty(app, signalbus):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush'])
    assert not result.output


def test_flush_pending(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush'])
    assert '1' in result.output
    assert 'processed' in result.output


def test_flush_error(app, signalbus_with_pending_error):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'flush'])
    assert isinstance(result.exception, ValueError)


def test_show_signals(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'signals'])
    assert 'Signal' == result.output.strip()


def test_show_pending(app, signalbus_with_pending_signal):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['signalbus', 'pending'])
    assert 'Signal' in result.output
    assert 'Total pending: 1' in result.output
