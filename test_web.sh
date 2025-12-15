#!/bin/bash

# Simple smoke test for web container
curl -f http://localhost:8080 > /dev/null
