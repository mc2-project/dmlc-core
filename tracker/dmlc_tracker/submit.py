"""Job submission script"""
from __future__ import absolute_import

import logging
from . import opts
from . import local
from . import mpi
from . import sge
from . import yarn
from . import mesos
from . import ssh
from . import rpc

def config_logger(args):
    """Configure the logger according to the arguments

    Parameters
    ----------
    args: argparser.Arguments
       The arguments passed in by the user.
    """
    fmt = '%(asctime)s %(levelname)s %(message)s'
    if args.log_level == 'INFO':
        level = logging.INFO
    elif args.log_level == 'DEBUG':
        level = logging.DEBUG
    else:
        raise RuntimeError("Unknown logging level %s" % args.log_level)

    if args.log_file is None:
        logging.basicConfig(format=fmt, level=level)
    else:
        logging.basicConfig(format=fmt, level=level, filename=args.log_file)
        console = logging.StreamHandler()
        console.setFormatter(logging.Formatter(fmt))
        console.setLevel(level)
        logging.getLogger('').addHandler(console)

def main():
    """Main submission function."""
    args = opts.get_opts()
    config_logger(args)
    fun_submit = {
        'local': local.submit,
        'sge': sge.submit,
        'yarn': yarn.submit,
        'mpi': mpi.submit,
        'mesos': mesos.submit,
        'ssh': ssh.submit,
        'rpc': rpc.submit,
    }
    if args.cluster in fun_submit:
        fun_submit[args.cluster](args)
    else:
        raise RuntimeError('Unknown submission cluster type %s' % args.cluster)
