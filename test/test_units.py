from main import main

def test_auto():
    res = main(None)
    assert res == 'ok'
