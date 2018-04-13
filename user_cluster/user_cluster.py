"""
user clustering algorithm
author : crystal
Date : 2018/4/11
"""
import sqlite3
import math

class GetVaule(object):
    """
        get the basic value and function from the the special database
    """

    def __init__(self, database):
        self.database = database
        self.conn, self.cursor = self.get_conn_cursor()

    # try to get the database connection and the database cursor
    def get_conn_cursor(self):
        try:
            conn = sqlite3.connect(self.database)
            cursor = conn.cursor()
            print "connect to the database successfully"
            return conn, cursor
        except:
            print "connect to the database failure"
            raise Exception

    # use the fetchall get the data which satisfy the condition
    # this function is bad, don't use it
    def get_data(self, sql):
        try:
            result = self.cursor.execute(sql)
            print "execute the sql successfully"
            return result
        except:
            print "there are something wrong about the sql"
            raise Exception

    # close the database
    def close(self):
        '''
            close the database after use
            you should always do it after you execute all of your sql
        :return: 0 stands for succeed
        '''
        try:
            self.close()
            print "close the database successfully"
            return 0
        except:
            print "close the database illegally"
            raise Exception


class GraphValue(object):
    def __init__(self, centroid, attribute):
        self.centroid = centroid
        self.attribute = attribute

    def get_centroid(self):
        return self.centroid

    def get_attribute(self):
        return self.attribute

    def __cmp__(self, other):
        for each in self.centroid:
            if each not in other.centroid:
                return -1
        return 0

class Graph(object):
    graph = []  # graph records the leave GraphValue object

    def add_ob(self, da, db):
        """
        add a new object into graph which combine da with db

        :param da: a GraphValue object
        :param db: a GraphValue object
        :return:
        """

        da.centroid.extend(db.centroid)
        da.attribute.extend(db.attribute)
        # value = GraphValue(da.centroid, da.attribute)
        # self.graph.append(value)

    def add_begin(self, d):
        self.graph.append(d)

    def minimal(self):
        """
        find the two object in the graph which has the most minimal distance

        :return: da, db
        """
        minimalvalue = 600000   # because the largest value is 548363
        da = GraphValue(["none"], [])
        db = GraphValue(["none"], [])
        for index1 in range(0, len(self.graph)-1):
            for index2 in range(index1+1, len(self.graph)):
                max_distince = d_max(self.graph[index1].get_attribute(), self.graph[index2].get_attribute())
                if minimalvalue > max_distince:
                    minimalvalue = max_distince
                    da = self.graph[index1]
                    db = self.graph[index2]
        return da, db

    def remove(self, ob):
        """
        remove the GraphValue in the graph
        :return:
        """
        self.graph.remove(ob)

def d_max(np, nq):
    """
    calculate the max distance between two GraphValue objects
    :param np: a GraphValue object
    :param nq: a GraphValue object
    :return:
    """
    max_value = 0
    # print np
    # print nq
    for i in range(0, len(np)):
        for j in range(0, len(nq)):
            tempvalue = math.log(np[i], 2) - math.log(nq[j], 2)
            if tempvalue > max_value:
                max_value = tempvalue
    return max_value

def user_cluster(database_name):
    """
    realize the user cluster algorithm in the algorithm 1
    :return: result_map
    """
    database = GetVaule(database_name)
    # get the user list
    get_userlist = "select distinct userid from jobs"
    conn, cursor = database.get_conn_cursor()
    cursor.execute(get_userlist)
    user_list = cursor.fetchall()
    # define the ecdfproblist
    ecdfprolist = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    # the main procedure
    ecdflist = []
    for user in user_list:
        # create the joblist of the user and sort the value asc
        get_tempjoblist = 'select runtime from jobs where userid = ?'
        tempjoblist = cursor.execute(get_tempjoblist, (user[0],)).fetchall()
        tempjoblist.sort(key=lambda temp: temp[0])
        # create the tempecdflist
        tempecdflist = [user[0]]
        tempjobtimelist = []
        tempjoblist_size = len(tempjoblist)
        for each in ecdfprolist:
            index = int(tempjoblist_size * each)
            if index < tempjoblist_size:
                tempjobtimelist.append(tempjoblist[index][0])
            else:
                tempjobtimelist.append(tempjoblist[tempjoblist_size - 1][0])
        print "get one tempjobtimelist and the user is %s" % user[0]
        print "get the tempjobtimelist is"
        print tempjobtimelist
        tempecdflist.append(tempjobtimelist)
        ecdflist.append(tempecdflist)
    print "have finished create the ecdflist"
    # create the G<d>
    graph_map = Graph()
    # create the cluster result
    result_map = []
    for each in ecdflist:
        d = GraphValue([each[0]], each[1])
        graph_map.add_begin(d)
    count_cluster = 0
    while len(graph_map.graph) > 1:
        da, db = graph_map.minimal()
        print len(graph_map.graph)
        print da.centroid
        graph_map.add_ob(da, db)
        print da.centroid
        temp_result_map = []
        for temp_result_each in da.centroid:
            temp_result_map.append(temp_result_each)
        result_map.append(temp_result_map)
        graph_map.remove(db)
        print len(graph_map.graph)
        count_cluster += 1
        print "finish cluster count %d" % count_cluster
        # break
    # write the result_map into a txt file
    with open("F:/usermodel/datasource/beihang/user_cluster.txt", "w") as cluster_flie:
        for each in result_map:
            temp_write = []
            for value in each:
                temp_write.append(value)
                temp_write.append(' ')
            temp_write.append('\n')
            cluster_flie.writelines(temp_write)
        print "write the result successfully"
    # write the ecdflist into a txt file
    conn.close()
    return result_map

def main():
    # the database save in "F:/usermodel/anon_jobs_sqlite/anon_jobs.db3"
    # if your database not in the same place you should change the path
    user_cluster("F:/usermodel/anon_jobs_sqlite/anon_jobs.db3")


# execute the main function
if __name__ == "__main__":
    main()
