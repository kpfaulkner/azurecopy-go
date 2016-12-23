# go run .\main.go -AzureDefaultAccountName="devstoreaccount1" -AzureDefaultAccountKey="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" -copy -source="c:/temp/data/" -dest="http://127.0.0.1:10000/devstoreaccount1/temp/" 

go run .\main.go -AzureDefaultAccountName="devstoreaccount1" -AzureDefaultAccountKey="Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==" -copy -dest="c:/temp/data/bbb/" -source="http://127.0.0.1:10000/devstoreaccount1/temp/" 
