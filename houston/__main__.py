
import argparse
from .commands import start, save, ignore, delete, fail, trigger, static_fire, skip, run_command

parser = argparse.ArgumentParser(prog="houston",
                                 description='Houston Command Line Tool. Executes all high-level commands. '
                                             'Full documentation available at https://callhouston.io/docs.')

commands = parser.add_subparsers(dest='command')

parser_start = commands.add_parser('start', description=start.__doc__)
parser_start.add_argument('-p', '--plan', type=str, help='Plan name', required=True)
parser_start.add_argument('-s', '--stage', type=str,
                          help='(optional) Comma separated list of stage names from which to start the '
                               'mission. Defaults to all stages with no upstream dependencies')
parser_start.add_argument('-i', '--ignore', type=str,
                          help='(optional) Comma separated list of stage names that should be ignored for the mission')

parser_save = commands.add_parser('save', description=save.__doc__)
parser_save.add_argument('-p', '--plan', type=str, required=True,
                         help='Plan file name, either local file path or Google Cloud Storage URI. '
                              'Plan can be either JSON or YAML')

parser_delete = commands.add_parser('delete', description=delete.__doc__)
parser_delete.add_argument('-p', '--plan', type=str, required=True, help='Plan name')

parser_skip = commands.add_parser('skip', description=skip.__doc__)
parser_skip.add_argument('-p', '--plan', type=str, required=True, help='Plan name')
parser_skip.add_argument('-m', '--mission_id', type=str, required=True, help='Mission ID')
parser_skip.add_argument('-s', '--stage', type=str, required=True,
                         help='Comma separated list of stage names to be marked as completed without running')
parser_skip.add_argument('-iup', '--ignore-dependencies', dest="ignore_dependencies", type=bool,
                         help='If true, ignore upstream stages', default=False)
parser_skip.add_argument('-idown', '--ignore-dependants', dest="ignore_dependants", type=bool,
                         help='If true, ignore downstream stages', default=False)

parser_ignore = commands.add_parser('ignore', description=ignore.__doc__)
parser_ignore.add_argument('-p', '--plan', type=str, required=True, help='Plan name')
parser_ignore.add_argument('-m', '--mission_id', type=str, required=True, help='Mission ID')
parser_ignore.add_argument('-s', '--stage', type=str,
                           help='(optional) Comma separated list of stage names to be ignored. Defaults to all stages')

parser_fail = commands.add_parser('fail', description=fail.__doc__)
parser_fail.add_argument('-p', '--plan', type=str, required=True, help='Plan name')
parser_fail.add_argument('-m', '--mission_id', type=str, required=True, help='Mission ID')
parser_fail.add_argument('-s', '--stage', type=str, required=True,
                         help='Comma separated list of stage names to be marked as failed')

parser_trigger = commands.add_parser('trigger', description=trigger.__doc__)
parser_trigger.add_argument('-p', '--plan', type=str, required=True, help='Plan name')
parser_trigger.add_argument('-m', '--mission_id', type=str, required=True, help='Mission ID')
parser_trigger.add_argument('-s', '--stage', type=str, required=True,
                            help='Comma separated list of stage names to be triggered')
parser_trigger.add_argument('-iup', '--ignore-dependencies', dest="ignore_dependencies", type=bool,
                            help='If true, ignore upstream stages', default=False)
parser_trigger.add_argument('-idown', '--ignore-dependants', dest="ignore_dependants", type=bool,
                            help='If true, ignore downstream stages', default=False)

parser_static_fire = commands.add_parser('static-fire', description=static_fire.__doc__)
parser_static_fire.add_argument('-p', '--plan', type=str, required=True, help='Plan name')
parser_static_fire.add_argument('-s', '--stage', type=str, required=True, help='Name of the stage to be triggered')


args = vars(parser.parse_args())

if args['command'] is not None:
    run_command(args['command'], **args)

else:
    parser.print_help()
