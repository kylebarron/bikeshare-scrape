#! /usr/bin/env bash
# Project: Bikeshare-scrape
# Program: 00server_setup.sh
# Author:  Kyle Barron <barronk@mit.edu>
# Created: 12/4/2017, 10:33:12 PM
# Purpose: Create automated script to set up remote working environment

sudo apt update
sudo apt upgrade

# Create sudo password (makes it easier to have zsh as default shell)
# sudo passwd ****

# Install Zsh and add my personal configuration
sudo apt install zsh
sh -c "$(curl -fsSL https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh)"
sudo chsh -s /bin/zsh kyle

sed -i -e 's/ZSH_THEME="robbyrussell"/ZSH_THEME="materialshell"/g' ~/.zshrc
wget https://raw.githubusercontent.com/kylebarron/dotfiles/master/zsh/materialshell.zsh-theme \
    -O ~/.oh-my-zsh/themes/materialshell.zsh-theme
wget https://raw.githubusercontent.com/kylebarron/dotfiles/master/git/gitconfig_desktop \
    -O ~/.gitconfig
    
# Allow for copy-paste
sudo apt install -y xclip
echo "alias clip='xclip -sel clip'" >> ~/.zshrc
source ~/.zshrc

# Install miniconda
wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh -b
echo 'export PATH="$HOME/miniconda3/bin:$PATH"' >> ~/.zshrc
source ~/.zshrc
rm Miniconda3-latest-Linux-x86_64.sh

# Install python packages
pip install pandas psycopg2 sqlalchemy

# Install Jupyter
conda install jupyter
jupyter notebook --generate-config
sed -i -e 's/#c.NotebookApp.open_browser = True/c.NotebookApp.open_browser = False/g' \
    ~/.jupyter/jupyter_notebook_config.py
# Add manual token
## Hydrogen remote kernels should now work.

# Install Postgres 10
echo 'deb http://apt.postgresql.org/pub/repos/apt/ xenial-pgdg main' | \
    sudo tee /etc/apt/sources.list.d/pgdg.list
wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | \
  sudo apt-key add -
sudo apt-get update
sudo apt install postgresql-10

# Set up postgres user
# https://www.digitalocean.com/community/tutorials/how-to-install-and-use-postgresql-on-ubuntu-16-04
sudo -u postgres createuser -d -r -s kyle
sudo -u postgres createdb kyle

# Make ssh key to use with Github
ssh-keygen -t rsa

# Download this git repo
mkdir -p ~/research/personal
cd ~/research/personal && git clone git@github.com:kylebarron/bikeshare-scrape.git
