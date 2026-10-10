import sys
from .store import migrate
if __name__=='__main__':
    if sys.argv[1:]!=['migrate']:raise SystemExit('python -m app.cli migrate')
    migrate();print('Metadata schema ready')
