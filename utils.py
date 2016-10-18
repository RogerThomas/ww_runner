K = 273.15
MAX_TIME = 24 * 7 - 1


def is_in_ww_range(ww_time, ww_min, ww_max):
    if ww_min <= ww_time <= ww_max:
        return True
    if ww_min > ww_max and ww_min <= ww_time + MAX_TIME <= ww_max + MAX_TIME:
        print(ww_time, ww_min, ww_max)
        print('in here!')
        return True
    return False


def get_index(value, min_val, ideal, max_val):
    if value < ideal:
        x2 = min_val
    elif value > ideal:
        x2 = max_val
    else:
        return 1
    try:
        m = 1.0 / (ideal - x2)
    except ZeroDivisionError:
        m = 0
    return max(0, m * value - m * x2)


def get_ww_index(wd_obj, ud_obj):
    weather_dict = dict(
        rain=wd_obj['rain']['3h'],
        cloud=wd_obj['clouds']['all'],
        temp=wd_obj['main']['temp'] - K,
        wind=wd_obj['wind']['speed'],
    )
    index = 0.0
    weight_sum = 0.0
    for index_key, weighting in ud_obj['weights'].items():
        tmp_index = get_index(weather_dict[index_key], *ud_obj[index_key])
        index += (tmp_index * weighting)
        weight_sum += weighting
    index /= weight_sum
    return index


def map_func(weather_data_bc, ud_obj):
    min_size = ud_obj['min_size']
    time_ranges = ud_obj['time_ranges']
    current_time_range = time_ranges.pop(0)

    wws = []
    current_ww = None
    for wd_obj in weather_data_bc.value:
        ww_time = wd_obj['ww_time']
        in_range = is_in_ww_range(ww_time, *current_time_range)
        if in_range:
            ww_index = get_ww_index(wd_obj, ud_obj)
        if in_range and ww_index > 0.65:
            if current_ww is None:
                current_ww = dict(start=ww_time, ww_index=[ww_index], finish=ww_time)
            else:
                current_ww['finish'] = ww_time
                current_ww['ww_index'].append(ww_index)
        else:
            if current_ww and current_ww['finish'] - current_ww['start'] > min_size:
                wws.append(current_ww)
                current_ww = None
            if ww_time > current_time_range[1]:
                try:
                    current_time_range = time_ranges.pop(0)
                except IndexError:
                    break
    return wws