def sizeof_fmt(num, suffix='B'):
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


def pretty_time_delta(seconds):
    # https://gist.github.com/thatalextaylor/7408395
    int_seconds = int(seconds)
    milliseconds = seconds - int_seconds
    days, seconds = divmod(int_seconds, 86400)
    hours, seconds = divmod(int_seconds, 3600)
    minutes, seconds = divmod(int_seconds, 60)
    if days > 0:
        return '{}d{}h{}m{}s{:0.2f}ms'.format(days, hours, minutes, seconds, milliseconds)
    if hours > 0:
        return '{}h{}m{}s{:0.2f}ms'.format(hours, minutes, seconds, milliseconds)
    if minutes > 0:
        return '{}m{}s{:0.2f}ms'.format(minutes, seconds, milliseconds)
    return '{}s{:0.2f}ms'.format(seconds, milliseconds)
