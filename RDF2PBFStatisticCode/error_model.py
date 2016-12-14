import osm_object_model as osm


class RelationErrorResults(object):
    """docstring for BucketResult"""

    def __init__(self):
        super(RelationErrorResults, self).__init__()
        # Contains the string of relation Identifer mapped to a RelationWithMissingMembers
        self.identifierForRelationWithMissingMembers = {}

    def isRelationListed(self, targetRelationIdentifer):
        return self.identifierForRelationWithMissingMembers.has_key(targetRelationIdentifer)

    def addRelation(self, targetRelation):

        targetRelationIdentifer = targetRelation.getRelationIdentifer()
        targetRelationMembers = targetRelation.getRelationMembers()

        # If the relation already exists
        if self.isRelationListed(targetRelationIdentifer) == True:

            # combine relation member list
            self.identifierForRelationWithMissingMembers[targetRelationIdentifer].addRelationMembers(
                targetRelationMembers)
        else:

            # Add target Relation to error list
            self.identifierForRelationWithMissingMembers[targetRelationIdentifer] = targetRelation

    def addMissingRelationMember(self, inputRelationIdentifer, inputMissingRelationMember):

        # Variable Declarations
        targetRelation = None

        if self.isRelationListed(inputRelationIdentifer) == True:

            # Retrieve Listed Relation
            targetRelation = self.identifierForRelationWithMissingMembers.get(inputRelationIdentifer)

        else:

            # Create new relation with a missing relation member
            targetRelation = osm.Relation(inputRelationIdentifer)

            # Add new relation
            self.addRelation(targetRelation)

        # Add missing relation member
        targetRelation.addRelationMember(inputMissingRelationMember)

    def getRelationsWithMissingMembers(self):

        return self.identifierForRelationWithMissingMembers.values()

    def add(self, otherRelationErrorResults):

        # Manual Add
        for relation in otherRelationErrorResults.getRelationsWithMissingMembers():
            self.addRelation(relation)

    def size(self):
        return len(self.identifierForRelationWithMissingMembers.keys())


class WayErrorResults(object):
    """docstring for WayErrorResults"""

    def __init__(self):
        super(WayErrorResults, self).__init__()
        self.identifierForWayWithMissingNodes = {}

    def isWayListed(self, targetWayIdentifier):

        return self.identifierForWayWithMissingNodes.has_key(targetWayIdentifier)

    def addWay(self, targetWay):

        targetWayIdentifer = targetWay.getWayIdentifier()
        targetWayNodeList = targetWay.getNodes()

        # If the relation already exists
        if self.isWayListed(targetWayIdentifer) == True:

            # Combine node lists
            self.identifierForWayWithMissingNodes[targetWayIdentifer].addNodes(targetWayNodeList)
        else:

            # Add target Relation to error list
            self.identifierForWayWithMissingNodes[targetWayIdentifer] = targetWay

    def addMissingNode(self, inputWayIdentifier, inputMissingNode):

        # Variable Declarations
        targetWay = None

        if self.isWayListed(inputWayIdentifier) == True:

            # Retrieve Listed Way
            targetWay = self.identifierForWayWithMissingNodes.get(inputWayIdentifier)

        else:

            # Create new way with a missing node
            targetWay = osm.Way(inputWayIdentifier)

            # Add new Way
            self.addWay(targetWay)

        # Add missing node
        targetWay.addNode(inputMissingNode)

    def getWaysWithMissingNodes(self):

        return self.identifierForWayWithMissingNodes.values()

    def add(otherWayErrorResults):

        # Manual Add
        for way in otherWayErrorResults.getWaysWithMissingNodes():
            self.addWay(way)

    def size(self):
        return len(self.identifierForWayWithMissingNodes.keys())
