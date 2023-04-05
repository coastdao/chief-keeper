#!/usr/bin/env bash

cd "$(dirname "$0")"

set -e

if [ ! "$(command -v python3.6)" ]; then
    echo "Python 3.6 is required to run this script."
fi

rm -rf _virtualenv
virtualenv --python="$(which python3.6)" _virtualenv
. _virtualenv/bin/activate

# The advantage of using this method, in contrary to just calling `pip3 install -r requirements.txt` several times,
# is that it can detect different versions of the same dependency and fail with a "Double requirement given"
# error message.
pip install $(cat requirements.txt $(find lib -name requirements.txt | sort) | sort | uniq | sed 's/ *== */==/g')
