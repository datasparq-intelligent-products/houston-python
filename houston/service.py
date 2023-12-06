"""High level API for services that carry out Houston tasks or commands.
"""

import os
import time
import json
from typing import Optional, Callable
from logging import getLogger, Logger

from .client import Houston
from .commands import run_command, wait, prepare_params
from .exceptions import HoustonClientError


def execute_service(
        event: dict,
        func: Callable,
        name: str = "unnamed",
        auth: Optional[dict] = None,
        time_limit_seconds: int = 300,
        wait_callback: Optional[Callable[..., bool]] = None,
        wait_interval_seconds: int = 10,
        log: Optional[Logger] = None,
        client_cls: Callable = Houston,
):
    """Executes a Houston stage or Houston command based on the event provided.

    :param event: Dictionary containing the arguments to this service. Could contain commands, mission arguments (for
                  running a stage in a mission), or arguments to `func` (for running without Houston). For more
                  information, see https://github.com/datasparq-ai/houston/blob/main/docs/services.md
    :param func: Function to execute. It can take any arguments contained within `event`.
    :param name: (optional) Friendly name for the service.
    :param auth: (optional) Map of service name to authentication parameters. See
                 https://github.com/datasparq-ai/houston/blob/main/docs/service_trigger_methods.md for
                 details on how to provide authentication for each type of authenticated trigger.
    :param time_limit_seconds: (optional) Maximum time the function can run for in a single invocation of the service.
    :param wait_callback: (optional)  When running the 'wait' command, this function will be used to check whether the
                          stage is finished or still running. It should return true or false. It can take any arguments
                          returned by `func`.
    :param wait_interval_seconds: (optional) For the 'wait' command, the time to wait between running the wait callback.
    :param log: (optional) Logger to use.
    :param client_cls: (optional) The Houston client class to use, i.e. Houston, GCPHouston, or AzureHouston.
    :return:
    """
    start = time.time()  # start time of the service used by wait callback
    if log is None:
        log = getLogger(os.getenv('HOUSTON_LOG_NAME', "houston"))

    if 'plan' not in event:  # if not using Houston
        log.info(f"No plan specified; running without Houston.")

        params = json.loads(event['params']) if 'params' in event else event
        params = prepare_params(params, func, houston_context=None)

        func_res = func(**params)

        if wait_callback is not None:
            log.info("Wait callback is defined but it is not currently possible to wait indefinitely "
                     "without Houston - will wait as long as possible in this invocation.")
            params = prepare_params(func_res, wait_callback, event)
            while not wait_callback(**params):
                log.info("Wait callback returned False. Waiting will continue.")
                time.sleep(10)
            log.info("Wait callback returned True. Waiting finished.")

        log.info(f"Finished {name}.")
        return func_res

    log.info(f"Initialising Houston client for plan '{event['plan']}'.")

    h = client_cls(plan=event['plan'], auth=auth)

    #
    # houston commands
    #

    if 'command' in event:
        event_params = dict(**event)
        command = event_params.pop('command')
        plan = event_params.pop('plan')
        log.info(f"Executing command '{command}'.")
        if run_command(command, plan=plan, client=h, **event_params, wait_callback=wait_callback,
                       start_time=start, time_limit_seconds=time_limit_seconds,
                       wait_interval_seconds=wait_interval_seconds):
            return  # end the cloud function if required for the command

    #
    # start stage
    #

    if 'stage' not in event:
        raise HoustonClientError("Event doesn't contain 'stage' attribute. Can't start a stage.")
    if 'mission_id' not in event:
        raise HoustonClientError("Event doesn't contain 'mission_id' attribute. "
                                 "A stage can't be started without knowing which mission it belongs to.")

    try:
        h.start_stage(event['stage'], event['mission_id'], ignore_dependencies=event.get("ignore_dependencies", False))
    except HoustonClientError:
        log.info("Stage has already started - stopping")
        return

    log.info(f"houston stage '{event['stage']}' started successfully")

    #
    # run operation
    #

    params = prepare_params(h.get_params(event['stage'], mission_id=event['mission_id']), func, houston_context=event)
    log.info(f"Loaded stage params: {params}")

    try:
        func_res = func(**params)

    except BaseException:
        log.error(f"Exception has occurred in stage '{event['stage']}' within in {name} Cloud Function.")
        log.error(f"Marking stage as failed.")
        h.fail_stage(event['stage'], event['mission_id'])
        raise

    if wait_callback is not None:  # check if waiting is required
        wait(**event, client=h, wait_callback=wait_callback, wait_params=func_res, start_time=start,
             time_limit_seconds=time_limit_seconds, wait_interval_seconds=wait_interval_seconds)
        return func_res  # end

    #
    # end stage
    #

    res = h.end_stage(event['stage'], mission_id=event['mission_id'],
                      ignore_dependencies=event.get('ignore_dependants', False))

    h.trigger_all(res.get('next', []), mission_id=event['mission_id'])

    log.info(f"Finished {name}.")

    return func_res
