# -*- coding: utf-8 -*-
from src.parse_jra_race import parse_jra_race
race_dict, results, horses, jockeys, trainers = parse_jra_race('data/raw/jra/race_202405020511.html')
for i,(k,v) in enumerate(trainers.items()):
    if i<3:
        print(repr(k), repr(v))
