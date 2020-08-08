from datetime import datetime, timedelta


def get_current_date_str():
    now = datetime.now()

    ret = now.strftime("%Y%m%d")

    return ret


def get_delta_date_str(delta):
    now = datetime.now()

    delta_days = timedelta(days=1) * delta

    ret = (now + delta_days).strftime("%Y%m%d")

    return ret


def get_current_hour_str():
    now = datetime.now()

    ret = now.strftime("%Y%m%d%H")

    return ret


def get_delta_hour_str(delta):
    now = datetime.now()

    delta_hours = timedelta(hours=1) * delta

    ret = (now + delta_hours).strftime("%Y%m%d%H")

    return ret


# yqc_2020080618.0.log
def make_log_file_names():
    last_hour = get_delta_hour_str(-1)
    log_file_name = 'yqc_' + str(last_hour) + '.*.log'
    return log_file_name


def main():
    current_date_str = get_current_date_str()
    print(f"current_date_str={current_date_str}")

    delta_day_str = get_delta_date_str(-1)
    print(f"delta_day_str={delta_day_str}")

    print("-" * 160)

    current_hour_str = get_current_hour_str()
    print(f"current_hour_str={current_hour_str}")

    delta_hour_str = get_delta_hour_str(-1)
    print(f"delta_hour_str={delta_hour_str}")

    print("-" * 160)
    log_file_name = make_log_file_names()
    print(f"log_file_name={log_file_name}")


if __name__ == '__main__':
    main()
