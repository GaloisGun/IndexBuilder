# IndexBuilder
Index Builder for the search ads application.

## 1. Download code
````
git clone git@github.com:GaloisGun/IndexBuilder.git
````

## 2. Build tables in mySQL DB

Build ad table
````
ad
adID
campaignID
keywords
bidPrice
price
thumbnail
description
brand
detail_url
category
title
````

Build campaign table
````
campaignID
budget
````

## 3. Start mysql and memecached

## 4. Run

> Change input path and output path in the main method for your own

````
cd ./IndexBuilder/out/artifacts/IndexBuilder_jar
java -jar IndexBuilder.jar
````

