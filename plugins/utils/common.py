def chunkize(total_items, first=1, chunksize=100):
    _first = first
    _second = chunksize
    while _first < total_items:
        yield [_first, _second]
        _first += chunksize
        _second += chunksize