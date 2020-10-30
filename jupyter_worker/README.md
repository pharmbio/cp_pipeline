in notebook image:

apt-get install texlive-xetex texlive-fonts-recommended texlive-generic-recommended



# when exporting, first run the notebook
jupyter nbconvert --to notebook --execute /pwd/hello.ipynb

# then export the already executed notebook as a pdf
jupyter nbconvert --to pdf /pwd/hello.nbconvert.ipynb












