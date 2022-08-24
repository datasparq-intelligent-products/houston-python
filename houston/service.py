"""High level API for services that carry out Houston tasks or commands.
"""

import os
import time
import json
from typing import Optional, Callable
from logging import getLogger, Logger

from houston.client import Houston
from houston.commands import run_command, wait, prepare_params
from houston.exceptions import HoustonClientError


def execute_service(
        event: dict,
        func: Callable,
        name: str = "unnamed",
        time_limit_seconds: int = 300,
        wait_callback: Optional[Callable[..., bool]] = None,
        wait_interval_seconds: int = 10,
        log: Optional[Logger] = None,
        client_cls: Callable = Houston,
):
    """Executes a Houston stage or Houston command based on the event provided.

    :param event:
    :param func:
    :param log: Logger to use
    :param name:
    :param time_limit_seconds:
    :param wait_callback:
    :param wait_interval_seconds:
    :param client_cls: The Houston client class to use, i.e. Houston, GCPHouston, or AzureHouston.
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

    h = client_cls(plan=event['plan'])

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

    try:
        h.start_stage(event['stage'], event['mission_id'], ignore_dependencies=event["ignore_dependencies"])
    except HoustonClientError:
        log.info("Stage has already started - stopping")
        return

    log.info(f"houston stage '{event['stage']}' started successfully")

    #
    # run operation
    #

    params = prepare_params(h.get_params(event['stage']), func, houston_context=event)
    log.info(f"Loaded stage params: {params}")

    try:
        func_res = func(**params)

    except BaseException:
        log.error(f"Exception has occurred in stage '{event['stage']}' within in {name} Cloud Function.")
        log.error(f"Marking stage as failed.")
        h.fail_stage(event['stage'], event['mission_id'])
        raise

    if wait_callback is not None:  # check if waiting is required
        event['wait_params'] = func_res
        wait(event, h, wait_callback=wait_callback, start_time=start,
             time_limit_seconds=time_limit_seconds, wait_interval_seconds=wait_interval_seconds)
        return func_res  # end

    #
    # end stage
    #

    res = h.end_stage(event['stage'], mission_id=event['mission_id'], ignore_dependencies=event['ignore_dependants'])
    h.trigger_all(res['next'], mission_id=event['mission_id'])
    log.info(f"Finished {name}.")

    return func_res
