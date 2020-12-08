---
name: md_ref
about: Ticket d'initialisation d'une presta de métadonnées de référence
title: "[MD_REF] - Client"
labels: md_ref
assignees: SimonSAMPERE

---

*le texte en italique est à remplacer par le chef de projet*

# Prestation de Métadonnées de Référence - *Client*

Groupe cible : [*0929bd0968bc4e19a6b58f65bdb4dda8*](https://app.isogeo.com/groups/0929bd0968bc4e19a6b58f65bdb4dda8/dashboard/formats)

Deadline : *01/01/2021*

Contexte : *Quelques mots si le chef de projet a la déter*

## Catalogues à migrer

- [**BD Topo 3.0** (54eb129b4dd840ada8de710994a310dc)](https://app.isogeo.com/groups/504f49055abc4d0b9865038fbc99b44b/inventory/search?p=1&ob=%23relevance&od=des&q=catalog%3A54eb129b4dd840ada8de710994a310dc) vers *uuid catalogue cible* 
- [**Admin Express** (7dd4fd6da07f403ba4419390f3815362)](https://app.isogeo.com/groups/504f49055abc4d0b9865038fbc99b44b/inventory/search?p=1&ob=%23relevance&od=des&q=catalog%3A7dd4fd6da07f403ba4419390f3815362) vers *uuid catalogue cible*
- [**Route500** (0bd42e9d5a724704ab17127168262a85)](https://app.isogeo.com/groups/504f49055abc4d0b9865038fbc99b44b/inventory/search?p=1&ob=%23relevance&od=des&q=catalog%3A0bd42e9d5a724704ab17127168262a85) vers *uuid catalogue cible*
- [**BD Carto 3.2** (3f518621e6de4bda9eafb03a9ab27e60)](https://app.isogeo.com/groups/504f49055abc4d0b9865038fbc99b44b/inventory/search?p=1&ob=%23relevance&od=des&q=catalog%3A3f518621e6de4bda9eafb03a9ab27e60) vers *uuid catalogue cible*
- ...

## Critères de correspondance
*La plupart du temps, la correspondance se fait sur le critère du nom de la donnée. Dans cette rubrique il faut indiquer les informations nécesssaires pour établir cette correspondance entre la fiche source (celle d'Isogeo) et la cible (celle du client). Par exemple : la nom de la fiche cible est préfixé, le nom de la cible et de la source n'ont pas la même casse...*
- **Commun à tous les catalogues** : *le nom de la cible est écrit en minuscules*
- **BD Topo 3.0** : *même nom*
- **Admin Express** : *le nom de la cible est préfixé de "BDT_H_"*
- **Route500** : *le nom de la cible est préfixé de "DOREF.ROUTE_" et la cible est associé à ce catalogue : uuid catalogue* 

## Règles de migration

- **Commun à tous les catalogues** : *les évènements ne doivent pas être importés*
- **BD Topo 3.0** : *associer la cible au mots-clef "référentiel"*
- **Route500** : *associer la cible au catalogue "Voirie" (uuid du catalogue)*

## Suivi

- [ ] **BD Topo 3.0** : *table de correspondance prête*
- [ ] **Admin Express** : *pas encore scanné par le client*
- [x] **Route500** : *migré*
