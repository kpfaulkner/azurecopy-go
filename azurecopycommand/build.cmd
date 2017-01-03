go build -ldflags "-X main.Version=0.1.0" main.go
del azurecopy.exe
ren main.exe azurecopy.exe
