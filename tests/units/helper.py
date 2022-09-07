def keep_last_result(vals):
    val = None
    for val in vals:
        yield val
    while True:
        yield val
