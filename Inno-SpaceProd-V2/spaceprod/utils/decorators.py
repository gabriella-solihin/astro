import time

from inno_utils.loggers.log import format_title, log

from spaceprod.utils.space_context.spark import spark


def timeit(func):
    """
    Decorator to execute a method and update its
    execution time as an attribute, called `execution_time`,
    of the instance class
    Parameters
    ----------
    func: function to time

    Returns
    -------
    returns time, also prints time in console
    """

    def timed(*args, **kwargs):
        msg_func_name = func.__name__

        msg = f"Start running step: '{msg_func_name}'."
        log.info(format_title(msg))

        ts = time.time()
        result = func(*args, **kwargs)

        spark.catalog.clearCache()

        for (id, rdd) in spark._sc._jsc.getPersistentRDDs().items():
            rdd.unpersist()

        assert len(spark._sc._jsc.getPersistentRDDs().items()) == 0

        te = time.time()

        msg_dur = time.strftime("%H:%M:%S", time.gmtime(te - ts))

        msg = f"Execution time of '{msg_func_name}': {msg_dur}."
        log.info(format_title(msg))

        return result

    return timed


def spit_integ_test_help(func):
    def wrapper(*args, **kwargs):
        test_run_id_1 = kwargs.get("test_run_id")
        msg = f"Your function must take 'test_run_id' 3rd positional arg"
        cond_1 = test_run_id_1 is not None
        cond_2 = len(args) == 3
        assert cond_1 or cond_2, msg
        test_run_id = test_run_id_1 or args[2]

        msg = f"""
        to debug this run use:
        
            from tests.integration.config import generate_test_config
            config = generate_test_config("{test_run_id}")
            
        and use this config to run your failing code
        """

        log.info(f"Starting a run")
        log.info(msg)

        result = func(*args, **kwargs)

        log.info(f"Successfully completed this run")
        log.info(msg)

        return result

    return wrapper
