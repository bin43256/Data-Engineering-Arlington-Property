
## Your `config.cfg` should look like the following:

```ini
[AWS]  
KEY=<your aws key>
SECRET=<your aws secret>

[BUCKET]  
# Make sure to create an S3 bucket and change the name to your bucket name
BUCKET_NAME=data-engineering-arlington-property-sale

[APIS]
SALES_HISTORY = https://datahub-v2.arlingtonva.us/api/RealEstate/SalesHistory
DWELLINGS_GENERAL = https://datahub-v2.arlingtonva.us/api/RealEstate/ImprovementDwelling
DWELLINGS_INTERIOR = https://datahub-v2.arlingtonva.us/api/RealEstate/ImprovementInterior
OUTBUILDINGS = https://datahub-v2.arlingtonva.us/api/RealEstate/Outbuilding
PROPERTY = https://datahub-v2.arlingtonva.us/api/RealEstate/Property
PROPERTY_CLASS = https://datahub-v2.arlingtonva.us/api/RealEstate/PropertyClassType

