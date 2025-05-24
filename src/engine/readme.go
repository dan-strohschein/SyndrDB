package engine

/*

looping through the fields we can create the resulting json object:
{
"FieldName" : "FieldValue"
}

This is how we would store the data in the file.
Relationships are stored via keys

Filters applied tot eh first level first
then filters for the child objects are applied

Something like:
- SELECT Fields
- FROM BUNDLE A
- Include A.SomeRelationshipField
- WHERE
A.SOMEFIELD = SOMEVALUE &&
A.Relationship.RelationshipField = SomeOtherValue

To create a bundle:

CREATE BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}, {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>})

To update a bundle:
UPDATE BUNDLE "BUNDLE_NAME"
CHANGE FIELD "<OLDFIELDNAME"> TO {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}
ADD FIELD {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}
REMOVE FIELD "<FIELDNAME>"

To Drop a bundle:
DELETE BUNDLE "BUNDLE_NAME"
 -- This is only possible if there are no relationships to other bundles


To Setup a relationship between two bundles:
UPDATE BUNDLE "BUNDLE_NAME"
CREATE RELATIONSHIP "RELATIONSHIP_NAME"
FROM BUNDLE "BUNDLE_NAME"
TO BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}, {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>})

To update a relationship:
UPDATE RELATIONSHIP "RELATIONSHIP_NAME"
FROM BUNDLE "BUNDLE_NAME"
TO BUNDLE "BUNDLE_NAME"
WITH FIELDS ({"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>}, {"<FIELDNAME>", <FIELDTYPE>, <REQUIRED>, <UNIQUE>})

To Drop a relationship:
UPDATE BUNDLE "BUNDLE_NAME"
DELETE RELATIONSHIP "RELATIONSHIP_NAME"


The way this works is that first, we get a list of the documents from that bundle
Then, we filter out documents that do not match the filter
Then, if there is an include command, we get the documents from that included bundle
That match the keys from the field of first set of documents A
Then, we filter out THOSE documents that do not match the OTHER filter
Then, we return the documents that match the filter


*/
