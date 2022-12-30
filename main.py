import numpy as np
import queue
from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable

from fileIO import FileIO
from componentExtractor import ComponentExtractor
from secret.secret import dbaddr, dbport, dbid, dbpw

class vertex:
    def __init__(self, label:int, data:str, segment:int, serial:str):
        #label: 노드의 종류를 나타냄, 1:컴포넌트, 2:비디오 세그먼트, 3:유저
        self.label = label

        #data는 노드의 종류에 따라 내용이 달라진다.
        #1:컴포넌트 이름(위키피디아 타이틀명), 2:영상 제목 or 영상 주소, 3: 유저 id
        self.data = data

        #segment는 노드의 종류가 세그먼트인 경우에만 해당하는 파트 숫자를 입력, 나머지는 0
        self.segment = segment
        self.edgeList = list()

        #neo4j에 노드를 입력하기 편하게 쿼리에들어갈 노드번호를 저장한다.
        self.nodeSerial = serial

class edge:
    def __init__(self, weight):
        #경우에 따라서 가중치대신 세그먼드정보가 들어갈 수 있다.
        self.weight = weight

def getRelGraph(result:list, videoAdress:list):
    #result가 트리플형태로 변환하기 전 이라는 가정으로 시작
    #result: 3차원 리스트, (1)모든 영상에대한 (2)각 세그먼트별 (3)컴포넌트
    #videoAdress: 영상의 주소가 result에 들어있는 순서대로 저장되어있는 리스트
    #
    #딕셔너리 길이 = 16333244
    title2IdDict = FileIO().callDictTitle2Id()

    #컴포넌트 개수는 딕셔너리 길이와 같다
    #아직 생성되지 않은 노드는 -1
    #최대 id크기 = 70355177
    #id를 다시 시리얼번호를 부여하면 메모리 절약가능
    componentArr = np.zeros(70355178, dtype=int)-1
    
    #컴포넌트 리스트에 추가하면 컴포넌트 배열에 새로 추가되는 컴포넌트의 인덱스를 저장해서 바로 찾을 수 있도록 작성
    componentList = list()
    videoList = list()
    segCount = 0
    videoCount = 0
    vidserial = 0
    for video in result:
        videoList.append(list())
        for seg in video:
            #리스트에 비디오 노드를 추가
            #node4j 테스트용으로 vidserial추가해서 그래프입력할때에 번호 알 수 있게 설정
            videoList[videoCount].append(vertex(2,videoAdress[videoCount],segCount,"v"+str(vidserial)))
            vidserial+=1
            for compo in seg:
                #구간 별로 컴포넌트 노드 추가

                serial = title2IdDict[compo.encode("utf-8")]
                if componentArr[serial] == -1:#노드 생성
                    #node4j 테스트용으로 시리얼번호 저장
                    n = vertex(1, compo, 0, "c"+str(len(componentList)))
                    
                    #생성된 컴포넌트 노드에 비디오노드 연결
                    #컴포넌트 리스트의 인덱스값을 배열에 넣어준다
                    componentArr[serial] = len(componentList)
                    componentList.append(n)

                else:#이미 생성된 컴포넌트
                    n = componentList[componentArr[serial]]

                #컴포넌트 노드와 영상 노드를 연결
                #가중치는 현재 없다고 가정 이후에 상의를 통해 가중치값도 받아서 엣지객체로 만들어 넣어줄것
                n.edgeList.append(videoList[videoCount][segCount])
                videoList[videoCount][segCount].edgeList.append(n)
            segCount += 1
        videoCount += 1    
        segCount = 0
        

    return componentList, videoList

class App:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    #구본현이 수정함, 디비에 존재하는 비디오 아이디를 들고옴
    def getVideos(self, tx):
        rets = []
        result = tx.run("MATCH (n : Video) RETURN (n)")
        for record in result:
            rets.append(record[0]["data"])
        return rets
    #구본현이 수정함-----------------------------------------------------

    def create_graph(self, componentList, videoList):
        with self.driver.session(database="neo4j") as session:
            # Write transactions allow the driver to handle retries and transient errors

            #구본현이 수정함, 비디오 입력을 넣기 전에 먼저 디비에 있는 아이디를 걸러냄
            haveIds = session.read_transaction(self.getVideos)
            realVideoList = []
            for i in videoList:
                if i not in haveIds:
                    realVideoList.append(i)
            #구본현이 수정함-----------------------------------------------------

            result = session.write_transaction(
                self._create_graph, componentList, realVideoList)
            
    @staticmethod
    def _create_graph(tx, componentList, videoList):
        #자체제작 함수

        #모든 컴포넌트 노드 생성
        for component in componentList:
            query = (
            "MATCH (c:KnowledgeComponent) "
            "WHERE c.data = $component_name "
            "RETURN c.data AS name"
            )
            isExist = False
            result = tx.run(query, component_name=component.data)
            for row in result:
                if len(row) >0:
                    isExist = True
            
            if isExist:
                continue
            query = "CREATE (c"+":KnowledgeComponent { data: $component_data }) "
            tx.run(query,component_data = component.data)

        #모든 비디오 노드 생성    
        for segmentList in videoList:
            query = (
            "MATCH (v:Video) "
            "WHERE v.data = $video_address "
            "RETURN v.data AS address"
            )
            result = tx.run(query, video_address=segmentList[0].data)
            isExist = False
            for row in result:
                if len(row) >0:
                    isExist = True

            
            if isExist:
                continue
            query = "CREATE (v"+":Video { data: $video_data }) "
            tx.run(query,video_data = segmentList[0].data)
            for segment in segmentList:
                query = (
                    "MATCH (v"+":Video { data: $video_data })"
                    "CREATE (s"+":Segment { data: $segment_data }) "
                    "CREATE (s)-[:PartOf]->(v)"
                )
                tx.run(query,segment_data = str(segment.segment),video_data = segment.data)

        #비디오노드 순회하면서 비디오노드와 컴포넌트 노드를 연결
        for segmentList in videoList:
            for segment in segmentList:
                for compo in segment.edgeList:
                    query = (
                        "MATCH (s: Segment {data: $segment_data})-[:PartOf]->(v: Video {data: $video_address}), (c: KnowledgeComponent{data: $component_data })-[:AppearedIn]->(s)"
                        
                        "RETURN c"
                    )
                    result = tx.run(query,segment_data = str(segment.segment), component_data = compo.data, video_address = segment.data)
                    isExist = False
                    for row in result:
                        if len(row) >0:
                            isExist = True
                    if isExist:
                        continue
                    query = (
                        "MATCH (s: Segment {data: $segment_data})-[:PartOf]->(v: Video {data: $video_address}), (c: KnowledgeComponent{data: $component_data })"
                        "CREATE (c)-[:AppearedIn]->(s)"
                        "RETURN s, c, v"
                    )
                    result = tx.run(query,segment_data = str(segment.segment), component_data = compo.data, video_address = segment.data)
                    
                    #비디오노드와 컴포넌트 노드 연결시키면서 콘솔창에 출력
                    try:
                        for row in result:
                            print("Created relationship between video:{v}, segment:{s}, component:{c}".format(
                            s=row["s"]["data"], c=row["c"]["data"], v=row["v"]["data"]))
                    
                    # Capture any errors along with the query and data for traceability
                    except ServiceUnavailable as exception:
                        logging.error("{query} raised an error: \n {exception}".format(
                            query=query, exception=exception))
                        raise

    def find_person(self, person_name):
        with self.driver.session(database="neo4j") as session:
            result = session.read_transaction(self._find_and_return_person, person_name)
            for row in result:
                print("Found person: {row}".format(row=row))

    @staticmethod
    def _find_and_return_person(tx, person_name):
        query = (
            "MATCH (p:Person) "
            "WHERE p.name = $person_name "
            "RETURN p.name AS name"
        )
        result = tx.run(query, person_name=person_name)
        return [row["name"] for row in result]

    def delete_all_data(self):
        with self.driver.session(database="neo4j") as session:
            result = session.write_transaction(self._delete_all_node_and_relationship)
    
    
    def is_exist_component(tx, component_name):
        query = (
            "MATCH (c:KnowledgeComponent) "
            "WHERE c.data = $component_name "
            "RETURN c.data AS name"
        )
        result = tx.run(query, component_name=component_name)
        for row in result:
            if len(row) >0:
                return True
        return False
    @staticmethod
    def _delete_all_node_and_relationship(tx):
        query = (
            "match (a) optional match (a)-[r]-() delete a, r"
        )
        tx.run(query)

    

def insertIntoNeo4j(addressList, resultList):
    #addressList: 영상의 id의 리스트
    #resultList: 각 영상별 세그먼트별 knowledge component리스트(3차원 리스트)
    componentList, videoList = getRelGraph(resultList,addressList)
    print("insert into neo4j")
    #uri = "neo4j+s://8a488d74.databases.neo4j.io"
    uri = "neo4j://%s:%s"%(dbaddr, dbport)
    #dbpw = "nZjn1bV_6nEPqDMs6l4f5rAnOo81peh7osW0X5fjcVw"
    app = App(uri, dbid, dbpw)  

    #데이터 전체 삭제
    #필요한 경우에만 활성화 시킬것
    ##app.delete_all_data()

    #neo4j에 그래프 작성
    app.create_graph(componentList,videoList)
    app.close()

#유튜브 id를 받으면 세그먼트, 컴포넌트로 이루어진 2차원 리스트를 반환함
def segmentExtract(CE:ComponentExtractor, splitSec, keywordSize, hitBool, id):
    ret = []
    sett:queue.Queue = CE.idToSplitQueue(splitSec, id)
    #c = 1
    while sett.qsize() != 0:
        subInSec = sett.get()
        #---------------------------------------------------------
        inp = CE.preProcess(subInSec)
        #print("At # %d segment, (%d ~ %d minutes), %d words are collected. Start..." %(c, (splitSec * c - splitSec) / 60, (splitSec * c) / 60, len(inp)))
        #---------------------------------------------------------
        g = CE.graphProcess(inp)
        result = g.getAnnotation(keywordSize, hitBool)
        #---------------------------------------------------------
        #print("~Completed")
        #---------------------------------------------------------
        retTemp = []
        for i in range(len(result)):
            print("%d : %s"%(i + 1, result[i].name))
            retTemp.append(result[i].name)
        ret.append(retTemp)
        #---------------------------------------------------------
        #c += 1
    return ret

def main():
    CE = ComponentExtractor()
    #-----------------------------------
    splitSec = 300
    keywordSize = 5
    hitBool = True
    #-----------------------------------
    ret = []
    with open("./input.txt","r") as inp:
        ids = inp.read().split('\n')
    print("Extract start")
    for id in ids:
        if id == '':
            break
        print("%s ..." % (id.strip()))
        ret.append(segmentExtract(CE, splitSec, keywordSize, hitBool, id))
    print("Extract end")
    print("DB Insert start")    
    insertIntoNeo4j(ids, ret)
    print("DB Insert end")

if __name__ == '__main__':
    main()