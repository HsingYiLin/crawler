#!/bin/bash
export DISPLAY=:1
xvfb-run python3 ~/igquery/crawl/ig_crawler_list.py

wait

pkill -f "Xvfb"

