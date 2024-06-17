"""Commands are additional high-level methods to allow users or Houston integrated services to carry out common tasks
with a single command. A Houston integrated service will run a command when triggered with a message containing the
'command' attribute.

Full documentation https://github.com/datasparq-ai/houston/blob/main/docs/commands.md
"""

import os
import logging
from functools import wraps

from retry import retry
from .client import Houston
from .exceptions import HoustonException, HoustonClientError, HoustonPlanNotFound
from typing import *
import time
import json

HOUSTON_MAX_WAIT_INVOCATIONS = os.getenv('HOUSTON_MAX_WAIT_INVOCATIONS', 150)
log = logging.getLogger(os.getenv('HOUSTON_LOG_NAME', "houston"))
retry_wrapper = retry((OSError, AttributeError), tries=3, backoff=3, delay=3, logger=log)


def init_client(func):
    """Initialise Houston client if not defined in wrapped function arguments."""
    @wraps(func)
    def inner(plan: str = None, client: Houston = None, *args, **kwargs):

        if plan is None:
            if client is None:
                raise ValueError("Either plan or client must be provided to run a command.")
            else:
                plan = client.plan['name']

        try:
            if client is None:
                try:
                    from houston.plugin.gcp import GCPHouston
                    client = GCPHouston(plan)
                except ImportError:
                    pass

            if client is None:
                try:
                    from houston.plugin.azure import AzureHouston
                    client = AzureHouston(plan)
                except ImportError:
                    client = Houston(plan)

        except HoustonPlanNotFound:
            if func.__name__ == "delete":
                return  # if the plan was already deleted then the client can't be initialised
            else:
                raise

        res = func(plan, client, *args, **kwargs)
        return res
    return inner


@init_client
def start(plan: str, client: Houston, stages: Union[str, List[str]] = None, ignore: Union[str, List[str]] = None,
          stage=None, skip: Union[str, List[str]] = None, **kwargs) -> bool:
    """Start a new mission. Creates a new mission ID and then trigger the first stages or the requested stages.
    If the requested stages are not the first stages their upstream dependencies will be ignored.
    """

    mission_id = client.create_mission()

    if ignore is not None:
        if isinstance(ignore, str):
            ignore = [a.strip() for a in ignore.split(",")]
        for s in ignore:
            try:
                client.ignore_stage(s, mission_id)
            except HoustonException:
                pass

    if skip is not None:
        if isinstance(skip, str):
            skip = [a.strip() for a in skip.split(",")]
        for s in skip:
            try:
                client.skip_stage(s, mission_id)
            except HoustonException:
                pass

    if stages is not None or stage is not None:
        starting_stages = stages if stages is not None else stage
        if isinstance(starting_stages, str):
            starting_stages = [a.strip() for a in starting_stages.split(",")]
    else:  # if not stages - determine first stages
        starting_stages = [s['name'] for s in client.independent_stages]

    # trigger with dependencies ignored
    client.trigger_all(starting_stages, mission_id=mission_id, ignore_dependencies=True)

    log.info(f"Started new mission with stage{'s' if len(starting_stages) != 1 else ''}: {starting_stages}.")
    return True  # end


@init_client
def ignore(plan: str, client: Houston, mission_id: str, stages: Union[str, List[str]] = None,
           stage=None, **kwargs) -> bool:
    """Ignore the requested stages. If no stages are specified then every stage will be ignored (essentially stopping
    the mission. note: Houston cannot stop a stage that has already been started).
    """
    if stage is not None:
        stages = stage
    if stages is not None:
        stages = stages if isinstance(stages, list) else [a.strip() for a in stages.split(",")]
    else:
        stages = [s['name'] for s in client.plan['stages']]

    for s in stages:
        try:
            client.ignore_stage(s, mission_id)
        except HoustonException:
            pass  # we don't care if stage was already ignored
    log.info(f"Ignored stages: {', '.join(stages)}")
    return True  # end


@init_client
def static_fire(plan: str, client: Houston, stage: str, **kwargs) -> bool:
    """Run requested stage and in isolation; ignore dependencies and dependants.
    """
    mission_id = client.create_mission()

    client.trigger(dict(stage=stage, mission_id=mission_id, plan=plan,
                        ignore_dependencies=True, ignore_dependants=True))
    log.info(f"Started a new mission and triggered stage '{stage}' with all other stages ignored.")
    return True  # end


@init_client
def save(plan: Union[str, dict], client: Houston, **kwargs) -> bool:
    """Save a plan or update an existing plan.
    """
    client.save_plan()
    log.info(f"Saved Plan '{client.plan['name']}' 🚀")
    return True  # end


@init_client
def delete(plan: str, client: Houston, mission_id: str = None, **kwargs) -> bool:
    """Delete a plan or mission. If a mission ID is provided then only the mission will be deleted.
    When a plan is deleted, every mission that belonged to that plan is also deleted, even if the
    mission is currently in progress.
    """

    if mission_id is not None:

        # Get mission and delete
        mission = client.get_mission(mission_id)
        client.delete_mission(mission_id, safe=True)
        print(json.dumps(mission.raw))

    else:
        client.delete_plan(safe=True)
        log.info(f"Deleted Plan '{client.plan['name']}'.")

    return True  # end


@init_client
def skip(plan: str, client: Houston, stage: str, mission_id: str, stages: Union[str, List[str]] = None,
         **kwargs) -> bool:
    """Skip one or more stages. Skipped stages won't be run,
    and the mission will continue as if these stages don't exist.
    """
    stages = stages if stages is not None else stage
    if isinstance(stages, str):
        stages = [a.strip() for a in stages.split(",")]
    for s in stages:
        client.skip_stage(stage_name=s, mission_id=mission_id)
        log.info(f"Marked stage '{s}' as skipped.")
    return True  # end


@init_client
def fail(plan: str, client: Houston, mission_id: str, stages: Union[str, List[str]] = None,
         stage=None, **kwargs) -> bool:
    """Force a stage or stages to be marked as failed.
    """
    stages = stages if stages is not None else stage
    if isinstance(stages, str):
        stages = [a.strip() for a in stages.split(",")]
    for s in stages:
        try:
            client.fail_stage(s, mission_id)
        except HoustonException:
            log.warning(f"Failed to fail stage '{s}'. Stage may not exist.")
            pass

    log.info(f"Marked stages as failed: {', '.join(stages)}")
    return True  # end


@init_client
def wait(plan: str, client: Houston, stage: str, mission_id: str, wait_callback: Callable[..., bool], start_time: float,
         time_limit_seconds, wait_interval_seconds, wait_params: Optional[dict] = None,
         wait_invocation_count: int = 1, **kwargs) -> bool:
    """Continue a stage that has already started and use the `wait_callback` function provided to check if the stage
    has finished. If the `wait_callback` returns `True`, the stage will end.
    This command is used by Houston services when a running a stage that takes longer than the service's
    execution time limit. If the service is going to run out of time it will trigger itself and continue waiting in a
    new invocation.

    The triggering message can contain `wait_params` which will be used as parameters for the `wait_callback`. This
    command will trigger new runs a maximum of 500 times to prevent infinite loops. This can be changed with the
    `HOUSTON_MAX_WAIT_INVOCATIONS` environment variable.

    :param plan: Houston plan name to which the stage belongs.
    :param client: Houston client.
    :param stage: Name of the stage of the plan to be waited for.
    :param mission_id: Houston mission that this stage is a part of.
    :param wait_callback: A function that returns true when the task is finished and false when waiting should continue.
    :param start_time: Time the function started.
    :param time_limit_seconds: The maximum amount of time the service should wait for.
    :param wait_interval_seconds: Time to wait between running the wait callback.
    :param wait_params: Parameters to be given to the wait callback function.
    :param wait_invocation_count: A counter to track how many times a service has run the wait command for this stage.
    """

    if wait_invocation_count > HOUSTON_MAX_WAIT_INVOCATIONS:
        log.error(f"There have been over {HOUSTON_MAX_WAIT_INVOCATIONS} invocations for waiting! "
                  f"This could be an infinite loop; waiting will stop.")
        return True  # end

    log.info(f"⏳ Waiting for {stage} to finish. Time limit is {time_limit_seconds}s.")
    log.info(f"This is wait invocation number {wait_invocation_count}.")
    seconds_elapsed = time.time() - start_time
    wait_res = False
    wrapped_wait_callback = retry_wrapper(wait_callback)

    if wait_params is None:
        wait_params = dict()

    params = prepare_params(wait_params, wait_callback,
                            dict(plan=plan, mission_id=mission_id, stage=stage, command="wait",
                                 wait_invocation_count=wait_invocation_count))

    try:
        # run the waiting function until it returns True or time runs out
        while seconds_elapsed < time_limit_seconds and not wait_res:
            log.info(f"⏳ Not finished after {seconds_elapsed} seconds.")
            time.sleep(wait_interval_seconds)
            wait_res = wrapped_wait_callback(**params)
            seconds_elapsed = time.time() - start_time

    except BaseException as be:
        log.error(f"Exception has occurred while waiting for stage '{stage}' to complete: {be}.")
        log.error(f"Marking stage as failed.")
        client.fail_stage(stage, mission_id)
        raise be

    if wait_res:
        try:
            res = client.end_stage(stage, mission_id)
            client.trigger_all(res['next'], mission_id)
        except HoustonClientError:
            # fail silently for 'This stage has already been completed' errors after waiting ends as they are harmless
            log.info("Stage has already been completed - doing nothing")

        log.info(f"🏁 finished after {seconds_elapsed} seconds!")
        return True  # end

    else:
        log.info(f"⌛ Reached Cloud Function time limit.️ "
                 f"Waiting for stage '{stage}' will continue in new invocation.")

        # start new function to continue waiting
        event = dict(plan=plan, stage=stage, mission_id=mission_id, command="wait",
                     wait_invocation_count=wait_invocation_count + 1, wait_params=wait_params)
        client.trigger(event)

        return True  # end


@init_client
def trigger(plan: str, client: Houston, mission_id: str, stages: Union[str, List[str]] = None, stage=None,
            ignore_dependencies: bool = False, ignore_dependants: bool = False, **kwargs) -> bool:
    """Manually trigger a stage or stages in an in-progress mission. This should only be us"""
    stages = stages if stages is not None else stage
    if isinstance(stages, str):
        stages = [a.strip() for a in stages.split(",")]
    for s in stages:
        client.trigger(dict(stage=s, mission_id=mission_id, ignore_dependencies=ignore_dependencies,
                            ignore_dependants=ignore_dependants))
    log.info(f"Triggered stages: {', '.join(stages)}")
    return True  # end


# aliases for some commands
update = save
blastoff = start
scrub = ignore
dummy = skip


def run_command(command_name: str, plan: str = None, client: Houston = None, *args, **kwargs) -> bool:
    """Select and run a command given the name of a command.
    """
    # map command names to functions
    command_map = dict(
        start=start,
        blastoff=start,
        missionstart=start,
        missionsequencestart=start,
        ignore=ignore,
        exclude=ignore,
        scrub=scrub,
        staticfire=static_fire,
        save=save,
        update=update,
        delete=delete,
        dummy=dummy,
        skip=skip,
        fail=fail,
        wait=wait,
        trigger=trigger,
    )

    command_name = command_name.replace("-", "").replace("_", "").replace("plan", "")

    if command_name in command_map:
        return command_map[command_name](plan, client, *args, **kwargs)
    else:
        raise ValueError(f"Houston Cloud Function does not recognise command '{command_name}'. "
                         f"Valid commands: {', '.join(command_map.keys())}")


#
# utils
#


def prepare_params(params: dict, target_func: Callable, houston_context) -> dict:
    """Prepare parameters from a stage triggering event for use in a service function. This includes parsing of
    JSON objects, removal of unexpected arguments, and addition of a logger or context arguments if required.

    :param params: Dictionary containing pairs of parameter names and values.
    :param target_func: The Houston service function.
    :param houston_context: Object containing information about the context that the stage is being run in, e.g. the
                            triggering event, which contains the stage, plan name, and mission id.
    :return: Dictionary containing pairs of prepared parameter names and values.
    """
    if params is None:
        params = dict()

    if isinstance(params, str):
        try:
            params = json.loads(params)
        except json.decoder.JSONDecodeError:
            raise ValueError("`params` could not be parsed. Should be a dict or valid JSON.")

    import inspect
    (args, varargs, varkw, defaults, kwonlyargs, kwonlydefaults, annotations) = inspect.getfullargspec(target_func)

    if varkw is None:  # if function does not accept extra key word arguments
        # remove any values that aren't used by the function to avoid 'got an unexpected keyword argument', e.g. 'topic'
        params = {key: value for key, value in params.items() if key in args}

    if varargs is not None:
        log.warning(
            f"Houston service cannot use a function that expects '*{varargs}' because argument order cannot be "
            f"guaranteed. Please use named parameters or '**kwargs'.")

    # try to JSON parse every string param
    for key in params:
        if isinstance(params[key], str):
            try:
                params[key] = json.loads(params[key])
            except json.decoder.JSONDecodeError:
                pass

    # if wrapped function expects a log parameter then add it
    if ('log' in args or varkw is not None) and 'log' not in params:
        params['log'] = log

    # if wrapped function expects a houston_context parameter then add it
    if ('houston_context' in args or varkw is not None) and 'houston_context' not in params:
        params['houston_context'] = houston_context

    return params
