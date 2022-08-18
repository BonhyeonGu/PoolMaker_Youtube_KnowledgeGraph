#from ast import operator
from operator import itemgetter
from math import log2
from collections import defaultdict

from fileIO import FileIO

import time 
import datetime

def getDict(anchorRange:tuple, TargetID:tuple) -> dict:
    entDict = defaultdict(int)
    for i in range(anchorRange[0], anchorRange[1]):
        entDict[TargetID[i]]+=1
        
    return entDict


def calcEnt(entDict:dict, entireNum:int) -> float:
    sum=0
    for i in entDict.items():
        sum+=(i[1]/entireNum)*log2(i[1]/entireNum)

    return -sum

#-----------------------------------------------------------------------------------------------------------------------------------------------------
class Vertex:
    def __init__(self, type:int, name:str):
        self.type = type#0: mention, 1:concept
        self.name = name#위키페이지 타이틀명or ID
        self.PR0 = 0
        self.PR = [0.0,0.0]
        self.edges = list()
        self.pointTo = list()

class Edge:
    def __init__(self, type:int):
        self.P = -1#전이확률, 가중치 역할
        self.SR = -1#컨셉간의 간선에만 사용
        self.type = type#mention to concept(0) or concept to concept(1)
        #print("type = %d"%(self.type))

    @classmethod
    def conceptToConcept(cls,SR:float):#컨셉에서 컨셉으로 가는 간선의 생성자 역할
        temp = Edge(1)
        temp.SR = SR
        return temp

    def calcMtoC(self,entireAnchorNum:int,sameTargetPage:int):
        self.P = sameTargetPage/entireAnchorNum

class Graph:
    def __init__(self, candidateMention:list):#candidateMention: 멘션 후보
        self.mentionList = candidateMention
        self.MAXENTROPHY = 3.0
        self.IDX = 0
        #---------------------------------------------------
        self.FileIO = FileIO()#경로 넣을것!
        #---------------------------------------------------
    def makeAllNode_test(self, mentionVertex:list, conceptVertex:list, ignoreRepeatedWord:bool):
        timeStart = time.time()
        
        anchorData = self.FileIO.callDictFull()
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print("dataLoadTime:"+ result_list)
        #없는 텍스트인지 확인해봐야함

        li = self.mentionList
        
        mentionVertexAppend = mentionVertex.append
        conceptVertexAppend = conceptVertex.append

        
        sortedList = list()
        conceptCandidateList = list()
        anchorTextNum = list()
        i = -1
        GetPR0Den = self.FileIO.getPR0den
        
        #같은 단어 들어오는지 확인하기위한 변수
        mentionChecker = dict()
        for mention in li:
            i+=1
            #앵커텍스트로서 존재하지 않는 단어는 제외
            try:
                #n = 전체 앵커텍스트 개수
                n = anchorData[mention][1] - anchorData[mention][0]
            except KeyError:
                continue

            #같은 단어 들어오면 무시
            if ignoreRepeatedWord == True:
                try:
                    mentionChecker[mention]+=1
                    continue
                except KeyError:
                    mentionChecker[mention] = 1
            
            entDict = anchorData[mention][2]
            entrophy = calcEnt(entDict,n)
            #엔트로피 통과하는지 확인
            if entrophy >=self.MAXENTROPHY:
                continue
            anchorTextNum.append(n)
            #딕셔너리 정렬
            sortedList = sorted(entDict.items(), key = itemgetter(1), reverse=True )
            conceptCandidateList.append(tuple(sortedList[:20]))

            #멘션노드 생성
            nowMention = Vertex(0,mention)

            #pageDict = getDict(anchorTextRange[i], PageID)
            nowMention.PR0 = anchorData[mention][3]/GetPR0Den(mention)#Crawling에 만들어놓은거 그대로 사용

            mentionVertexAppend(nowMention)
        conceptDict = dict()
        conceptCandidateList = tuple(conceptCandidateList)
        anchorTextNum = tuple(anchorTextNum)
        i = -1
        for mentionNode in mentionVertex:
            i+=1
            j=-1
            
            for conceptCandidate in conceptCandidateList[i]:#하나의 멘션에 대한 컨셉들 수만큼 노드, 간선 만듬
                j+=1
                #ni >= 2 인 것만 컨셉노드 생성
                if conceptCandidate[1] < 2:
                    break

                #이미 만든 컨셉 노드중에 같은 노드가 존재하는지 확인
                try:
                    nowConcept = conceptDict[conceptCandidate[0]]
                except KeyError:
                    nowConcept = Vertex(1,str(conceptCandidate[0]))
                    conceptDict[conceptCandidate[0]] = nowConcept
                    conceptVertexAppend(nowConcept)


                edge = Edge(0)#mention to concept 엣지 생성
                edge.P = conceptCandidate[1] / anchorTextNum[i] #P(가중치) 계산
                #컨셉노드와 엣지 연결
                edge.dest = nowConcept
                edge.start = mentionNode

                mentionNode.edges.append(edge)#멘션노드와 엣지 연결
                nowConcept.pointTo.append(edge)#컨셉노드에 자신을가리키는 엣지 리스트에 추가
            #하나의 멘션에대한 컨셉노드 연결 끝
            
        #모든 멘션에대한 노드 만들기 끝 
          
#-------------------------------------------------------------------------------------------------------------------------------------------------------
    def makeAllNode(self, mentionVertex:list, conceptVertex:list, ignoreRepeatedWord:bool):
        timeStart = time.time()
        anchorTextRange = self.FileIO.anchorTextToRangeSingle(self.mentionList)
        anchorTextRange = tuple(anchorTextRange)
        #없는 텍스트인지 확인해봐야함

        TargetID = self.FileIO.callListAnchorTargetID()
        TargetID = tuple(TargetID)
        PageID = self.FileIO.callListNowPageID()
        PageID = tuple(PageID)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print("dataLoadTime:"+ result_list)
        li = self.mentionList
        
        mentionVertexAppend = mentionVertex.append
        conceptVertexAppend = conceptVertex.append

        
        sortedList = list()
        conceptCandidateList = list()
        anchorTextNum = list()
        i = -1
        NameEncoder = self.FileIO.nameEncode
        GetPR0Den = self.FileIO.getPR0den
        
        #같은 단어 들어오는지 확인하기위한 변수
        mentionChecker = dict()
        for mention in li:
            i+=1
            #앵커텍스트로서 존재하지 않는 단어는 제외
            if anchorTextRange[i] == -1:
                continue
            #같은 단어 들어오면 무시
            if ignoreRepeatedWord == True:
                try:
                    mentionChecker[mention]+=1
                    continue
                except KeyError:
                    mentionChecker[mention] = 1
            #n = 전체 앵커텍스트 개수
            n = anchorTextRange[i][1]-anchorTextRange[i][0]
            
            # try:
            #     with open("./entrophy/"+NameEncoder(mention)+".pkl","rb") as inpf:
            #         entDict = pickle.load(inpf)
            # except FileNotFoundError:
            #     entDict = getDict(anchorTextRange[i],TargetID)
            #     with open("./entrophy/"+NameEncoder(mention)+".pkl","wb") as outf:
            #         pickle.dump(entDict,outf)
            entDict = getDict(anchorTextRange[i],TargetID)
            entrophy = calcEnt(entDict,n)
            #엔트로피 통과하는지 확인
            if entrophy >=self.MAXENTROPHY:
                continue
            anchorTextNum.append(n)
            #딕셔너리 정렬
            sortedList = sorted(entDict.items(), key = itemgetter(1), reverse=True )
            conceptCandidateList.append(tuple(sortedList[:20]))

            #멘션노드 생성
            nowMention = Vertex(0,mention)

            pageDict = getDict(anchorTextRange[i], PageID)
            nowMention.PR0 = len(pageDict)/GetPR0Den(mention)#Crawling에 만들어놓은거 그대로 사용

            mentionVertexAppend(nowMention)
        conceptDict = dict()
        conceptCandidateList = tuple(conceptCandidateList)
        anchorTextNum = tuple(anchorTextNum)
        i = -1
        for mentionNode in mentionVertex:
            i+=1
            j=-1
            
            for conceptCandidate in conceptCandidateList[i]:#하나의 멘션에 대한 컨셉들 수만큼 노드, 간선 만듬
                j+=1
                #ni >= 2 인 것만 컨셉노드 생성
                if conceptCandidate[1] < 2:
                    break

                #이미 만든 컨셉 노드중에 같은 노드가 존재하는지 확인
                try:
                    nowConcept = conceptDict[conceptCandidate[0]]
                except KeyError:
                    nowConcept = Vertex(1,str(conceptCandidate[0]))
                    conceptDict[conceptCandidate[0]] = nowConcept
                    conceptVertexAppend(nowConcept)


                edge = Edge(0)#mention to concept 엣지 생성
                edge.P = conceptCandidate[1] / anchorTextNum[i] #P(가중치) 계산
                #컨셉노드와 엣지 연결
                edge.dest = nowConcept
                edge.start = mentionNode

                mentionNode.edges.append(edge)#멘션노드와 엣지 연결
                nowConcept.pointTo.append(edge)#컨셉노드에 자신을가리키는 엣지 리스트에 추가
            #하나의 멘션에대한 컨셉노드 연결 끝
            
        #모든 멘션에대한 노드 만들기 끝 
        
        del TargetID
        del PageID

    def makeEdgeCtoC(self, conceptVertex:list) -> None:
        N=1633324#전체 아이디 개수
        GetBackLinks = self.FileIO.getBacklinks
        backlink_list = list()
        
        #backlink집합을 가지는 튜플을 만든다
        for i in conceptVertex:
            backlink_list.append(GetBackLinks(i.name + "_backlinks.pickle"))
        backlink_tuple = tuple(backlink_list)

        i = -1
        for startVertex in conceptVertex:
            i+=1
            j=-1
            for endVertex in conceptVertex:
                j+=1
                if(i == j):#자기자신을 가리키는 간선 안생김
                    continue
                #i에서 j로
                
                SR = self.calcSR(backlink_tuple[i],backlink_tuple[j],N)

                if(SR > 0):#SR값이 0보다커야 간선 추가함
                    edge = Edge.conceptToConcept(SR)
                    edge.dest = endVertex
                    edge.start = startVertex
                    # oppositeEdge = Edge.conceptToConcept(SR)
                    # oppositeEdge.dest = conceptVertex[i]
                    # oppositeEdge.start = conceptVertex[j]

                    startVertex.edges.append(edge)
                    # conceptVertex[i].pointTo.append(oppositeEdge)
                    # conceptVertex[j].edges.append(oppositeEdge)
                    endVertex.pointTo.append(edge)

    def calcPR0(self, mentionVertex:list):
        sum = 0
        
        for i in mentionVertex:#z를 제외한 계산 완료
            sum +=i.PR0
        z = 1/sum

        for i in mentionVertex:#z를 곱해줘서 계산 완료
            i.PR0 *= z


    def calcPosibilityCtoC(self, conceptVertex:list):
        for i in conceptVertex:
            sum = 0
            for j in i.edges:
                sum += j.SR

            for j in i.edges:
                if sum-j.SR == 0:#컨셉 노드가 적은경우 0인 경우가 발생
                    j.P = 1#임시로 1로 지정하도록 변경
                else:
                    j.P = j.SR/(sum)

    def calcSR(self, start_set:set, end_set:set, N:int) -> float:
            
        sameNum = len(start_set & end_set)
            
        #집합 사이즈 저장
        startLen = len(start_set)
        endLen = len(end_set)

        #수식 계산
        SR = 0
        if sameNum == 0:#log2에 0이 들어가면 에러뜸
            return 0
        denominator = (log2(N) - log2(min(startLen,endLen)))
        numerator = (log2(max(startLen,endLen)) - log2(sameNum)) #분자
        #분모가 0인 경우가 발생할 수 있음. 임시로 0으로 처리하는걸로 해놓음
        if denominator == 0:
            SR = 0
        else:
            SR = 1- numerator / denominator
        
        return SR 

    def calcPR(self, repeat:int, mentionVertex:list, conceptVertex:list):
        #repeat:반복계산 횟수
        allVertex = mentionVertex + conceptVertex
        r =0.1
        for i in range(repeat):
            #print("repeat num: %d" %(i+1))
            newIdx = i%2
            oldIdx = (i+1)%2
            for vertex in allVertex:
                sum=0
                
                for edge in vertex.pointTo:
                    sum += edge.start.PR[oldIdx] * edge.P
                vertex.PR[newIdx] = r *vertex.PR0  + (1-r)*sum
                #print("name: "+ vertex.name + " PR: %lf"%( vertex.PR[newIdx]))
            
        self.IDX = newIdx
        # allPR = 0.0
        # for j in allVertex:
        #     allPR +=j.PR[self.IDX]

        # print("allPR: %lf"%(allPR))
        return

    def calcSupportConcept(self, mentionVertex:list):
        #멘션당 PR값이 가장 높은 하나의 노드를 제외하고 나머지 노드를 없앤다
        supportNode = set()
        for mNode in mentionVertex:
            maxPR = -1
            maxNode = -1
            for i in range(len(mNode.edges)):
                if maxPR < mNode.edges[i].dest.PR[self.IDX]:
                    maxPR = mNode.edges[i].dest.PR[self.IDX]
                    maxNode = i
            #멘션노드가 아무노드와 연결되어있지 않는경우 에러 발생

            if maxNode == -1:
                continue
            temp = mNode.edges[maxNode]
            mNode.edges[maxNode] = mNode.edges[0]
            mNode.edges[0]=temp
            if not(temp.dest in supportNode):
                supportNode.add(temp.dest)
            
        supportNode = list(supportNode)
        supportNode = sorted(supportNode, key = lambda node: node.PR, reverse=True)
        return supportNode

    def getAnnotation(self, numberOfAnnotation:int, ignoreRepeatedWord:bool = False):#text는 mention들의 리스트, numberOfAnnotation는 결과 단어 몇개 출력할지 정하는 변수
        #그래프 노드, 멘션노드에서 컨셉노드로 향하는 엣지 생성
        print("makeAllNode")
        timeStart = time.time()
        mentionVertex=[]#멘션 노드 저장장소
        conceptVertex=[]#컨셉 노드 저장장소
        self.makeAllNode_test(mentionVertex, conceptVertex, ignoreRepeatedWord)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)
        
        #컨셉노드끼리의 간선 이어야함
        print("makeEdgeCtoC")
        timeStart = time.time()
        self.makeEdgeCtoC(conceptVertex)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)
        #vg.visualize(mentionVertex,conceptVertex)
        #PR0 계산
        print("calcPR0")
        timeStart = time.time()
        self.calcPR0(mentionVertex)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)

        #P(c,c')계산
        print("calcPosibilityCtoC")
        timeStart = time.time()
        self.calcPosibilityCtoC(conceptVertex)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)

        #PR계산
        print("calcPR")
        timeStart = time.time()
        
        self.calcPR(100,mentionVertex,conceptVertex)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)

        #멘션노드당 하나씩 최고 PR값 높은 컨셉노드 구하기
        print("calcSupportConcept")
        timeStart = time.time()
        supportNodeList = self.calcSupportConcept(mentionVertex)
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)

        #ID를 타이틀로 변환시킬 준비
        print("getIDToTitle")
        timeStart = time.time()
        IDList=list()
        for i in supportNodeList[:numberOfAnnotation]:
            IDList.append(int(i.name))
        
        #ID를 타이틀로 변환
        IDList = self.FileIO.getIDToTitle(IDList)
        for i in range(len(IDList)):
            supportNodeList[i].name = IDList[i].decode('utf-8')
        timeEnd = time.time()
        sec = timeEnd - timeStart
        result_list = str(datetime.timedelta(seconds=sec))
        print(result_list)
        del IDList
        return supportNodeList[:numberOfAnnotation]
