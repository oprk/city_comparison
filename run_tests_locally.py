#!/usr/bin/env python
"""
A helper script to run test commands from the .travis.yml file locally
in the dev environment.
"""

import subprocess
# pylint: disable=E0401
import yaml

with open('.travis.yml', 'r') as filehandler:
  try:
    TRAVIS_CONFIG = yaml.safe_load(filehandler)
  except yaml.YAMLError as exc:
    print(exc)

TEST_COMMANDS = TRAVIS_CONFIG.get('script')

EXIT_ERRORS = False
for test_command in TEST_COMMANDS:
  try:
    subprocess.check_output(test_command, shell=True)
  except subprocess.CalledProcessError as process_error:
    print(test_command)
    print(process_error.output.decode())
    EXIT_ERRORS = True
    print('')

if EXIT_ERRORS:
  exit(1)
