cd ./doc
make clean
sphinx-apidoc -o ./modules ../mingmq
make html
ssh wobuhuizaibeishang@serv_pro "rm -rf ~/static_server/MingMQ"
scp -r ./_build/html/ wobuhuizaibeishang@serv_pro:~/static_server/MingMQ
cd ..