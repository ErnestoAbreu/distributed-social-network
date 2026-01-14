def is_in_interval(x, a, b, inclusive_end=False):
    """Returns True if x is in interval (a, b) or (a, b], otherwise False"""
    if a < b:
        if inclusive_end:
            return a < x and x <= b
        return a < x and x < b
    else: 
        if inclusive_end:
            return x > a or x <= b
        return x > a or x < b