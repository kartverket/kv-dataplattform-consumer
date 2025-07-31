# Beskrivelse
Et repo som inneholder kode for en modul som gjør at man kan interagere med kryptert data, som er delt via f.eks Delta Sharing.

## Bruk

For å installere biblioteket kjører man bare 
```sh
pip install kv-dataplatform-consumer
```

Testet med versjon 3.13.1

### Generer en egen nøkkel

Som konsument av kryptert data vil du trenge en egen asymmentrisk nøkkel for å dekryptere Kartverkets datakey (symmetriske nøkkel i steg1 under). Den kan du generere på følgende måte 

```python
from kv_dataplatform_consumer import crypto_utils
crypto_utils.generate_public_private_key()
```

Ta vare på `private_key` og lagre denne et sikkert sted hos deg selv, og del `public_key` med Kartverket. I første omgang kan den sendes manuelt til dine kontaktpersoner i Kartverket. 

Om du ikke kan bruke biblioteket kan du bruke følgende kommandoer

```
# Generer RSA 4096 private key
openssl genrsa -out private.pem 4096
# Print public key til fil
openssl rsa -in private.pem -pubout > public.pem
```

## Komme i gang med utvikling
- Man må ha pipx og poetry installert
- Deretter er det bare å kjøre `poetry install`
- Så kan man kjøre testene via `poetry run pytest`

## Forklaring på kryptoflyten
1. Generer en **unik** symmetrisk nøkkel = `key`
2. Generer en tilhørende **unik** UUID = `key_id`
3. Lagre denne et sikkert sted, slik at man senere kan slå opp på `key_id`
4. Oppretter / rekrypterer en tabell, der man for hver rad i gjenbruker nøkkelen key og genererer en **unik** nonce for (kolonne,rad) som skal krypteres. Tabellen vil lagres inn i et gjenbrukbart schema, f.eks pii_schema. Gitt en tabell kalt person med kolonnene navn og alder, og man skal kryptere navn, så vil man da ende opp med en tabell som ser slik ut:

| navn_enc | navn_nonce | alder | key_id |
| -------- | ---------- | ----- | ------ |
| qwkeoqwke | 12mkqqwek |   20  | ce-232 |
| qwkej123j | 1kocmmqwe |   30  | ce-232 |
| koko2k313 | cmk2kpqeq |   40  | ce-232 |

5. Deretter så vil man for hver consumer c ta inn deres public key = `pkc`, og da lage et entry, der man krypterer `key` med konsumenten sin offentlige nøkkel `pkc`. Da kan man lage et entry i `__keys__c__pii_schema` og en tabell som også heter `person`, som vil se slik ut: 

| key_id |     key     |
| ------ | ----------- |
| ce-232 | wqkeo123koq |

6. Konsumenten kan da ta i bruk pakken i repoet, og bruke `consume_table_from_share` funksjonen i `consume_share.py`. Den tar inn en gitt tabell i et skjema, og den asymmetriske private nøkkelen til konsumenten = `pc`. `consume_table_from_share` vil da gitt person-tabellen, joine sammen `pii_schema.person` og `__keys__c__pii_schema.person`. Deretter henter den ut en rad, der den fisker ut den gjeldende nøkkelen `key`. Den dekrypteres med `pc`. Deretter går den gjennom hver rad, og gitt `navn_enc` og `navn_nonce` så dekrypterer den raden med den symmetriske nøkkelen. Får da ut tabellen slik:

|    navn    | alder |
| ---------- | ----- |
|   August   |  20   |
|    Jonas   |  30   |
|  Joachim   |  40   |


