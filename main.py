from models import EmailAlert


def main(request):
    job = EmailAlert()
    job.run()
    return 'ok'
