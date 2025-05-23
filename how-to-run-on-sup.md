# How to run ingestor on supervisor

## Install nvm

curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash

## Load nvm

export NVM_DIR="$HOME/.nvm"
[ -s "$NVM_DIR/nvm.sh" ] && \. "$NVM_DIR/nvm.sh"  # This loads nvm
[ -s "$NVM_DIR/bash_completion" ] && \. "$NVM_DIR/bash_completion"  # This loads nvm bash_completion

# Install node 22 and yarn

nvm install 22
npm install --global yarn

# Install package dependencies

yum install make gcc gcc-c++ python3.11

# Make python 3.11 the default, otherwise you get walrus operator errors in some python scripts

rm /etc/alternatives/python3
ln -s /usr/bin/python3.11 /etc/alternatives/python3

## Install node modules

yarn install

## Run it!
