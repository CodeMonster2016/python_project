class Relation(object):
	"""docstring for Relation"""
	def __init__(self, inputRelationIdentifer):
		super(Relation, self).__init__()
		self.relationIdentifer = inputRelationIdentifer
		self.relationMembers = []

	def getRelationIdentifer(self):
		return self.relationIdentifer

	def getRelationMembers(self):
		return self.relationMembers
	
	def addRelationMember(self,inputRelationMember):
		self.relationMembers.append(inputRelationMember)

	def addRelationMembers(self,inputRelationMembers):
		self.relationMembers = self.relationMembers + inputRelationMembers

class RelationMember(object):
	"""docstring for RelationMember"""
	def __init__(self, inputMemberIdentifer,inputMemberType,inputMemberRole):
		super(RelationMember, self).__init__()
		self.memberIdentifer = inputMemberIdentifer
		self.memberType = inputMemberType
		self.memberRole = inputMemberRole

	def getMemberIdentifer(self):
		return self.memberIdentifer

	def getMemberType(self):
		return self.memberType

	def getMemberRole(self):
		return self.memberRole	

	def __str__(self):
		outputString = 'RelationMember --> Identfier = {identfier} | MemberType = {membertype} | MemberRole = {memberrole}'

		return outputString.format(identfier=self.memberIdentifer,membertype=self.memberType,memberrole=self.memberRole)
		
class Way(object):
	"""docstring for Way"""
	def __init__(self, inputWayIdentifier):
		super(Way, self).__init__()
		self.wayIdentifier = inputWayIdentifier
		self.nodeList = []

	def getWayIdentifier(self):
		return self.wayIdentifier

	def getNodes(self):
		return self.nodeList

	def addNode(self,inputNode):
		self.nodeList.append(inputNode)

	def addNodes(self,inputNodeList):
		self.nodeList = self.nodeList + inputNodelist

class Node(object):
	"""docstring for Node"""
	def __init__(self, inputNodeIdentifier):
		super(Node, self).__init__()
		self.nodeIdentifier = inputNodeIdentifier

	def getNodeIdentifier(self):
		pass
		


		