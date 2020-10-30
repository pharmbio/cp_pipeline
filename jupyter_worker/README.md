in notebook image:

apt-get install texlive-xetex texlive-fonts-recommended texlive-generic-recommended


# start debug session
docker run -e WORK_FOLDER="katt" -it -u root -v $(pwd):/pwd pharmbio/pharmbio-notebook:tf-2.1.0 bash

# when exporting, first run the notebook
jupyter nbconvert --to notebook --execute /pwd/hello.ipynb

# then export the already executed notebook as a pdf
jupyter nbconvert --to pdf /pwd/hello.nbconvert.ipynb












