cd ./doc
make clean
sphinx-apidoc -o ./ ../mingmq/
make html
cd ..
scp -r ./doc/_build/html wobuhuizaibeishang@serv_pro:~/static_server/MingMQ