# Beskrivelse Java

Et repo som inneholder kode for en modul som gjør at man kan interagere med kryptert data, som er delt via f.eks Delta Sharing.

## Installere Java

1. For å installere Java (på Ubuntu):
```sh
sudo apt update
sudo apt install openjdk-17-jdk
```

2. Finn Java installation path:
```sh
readlink -f $(which java) | sed "s:bin/java::"
```

Da får du noe som ligner på `/usr/lib/jvm/java-17-openjdk-amd64/`.

3. Sett `JAVA_HOME`:
```sh
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64/
export PATH=$JAVA_HOME/bin:$PATH
```

(Bytt ut pathen om du fikk noe annerledes i steg 2.)

## Komme i gang med utvikling

- Installere dependencies: `./gradlew build`
- Kjøre tester: `./gradlew test`
