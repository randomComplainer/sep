#!/bin/bash

readonly test_name=$1

RUST_LOG=debug RUST_LOG_SPAN_EVENTS=new,close cargo test ${test_name} -- --nocapture
