from hashlib import sha256
import json, sqlite3, threading, time, os, random, socket
from ellipticcurve.ecdsa import Ecdsa
from ellipticcurve.privateKey import PrivateKey
from ellipticcurve.publicKey import PublicKey
from ellipticcurve.signature import Signature

nodes = []


class Block:

    def __init__(self, timestamp=None, data=None):
        self.timestamp = timestamp or time.time()
        self.data = [] if data is None else data
        self.prevHash = None
        self.nonce = 0
        self.hash = self.getHash()

    def getHash(self):
        hash = sha256()
        hash.update(str(self.prevHash).encode('utf-8'))
        hash.update(str(self.timestamp).encode('utf-8'))
        hash.update(str(self.data).encode('utf-8'))
        hash.update(str(self.nonce).encode('utf-8'))
        return hash.hexdigest()

    def mine(self, difficulty):
        # while self.hash[:difficulty] != '0' * difficulty:
        self.nonce += random.randint(1, 1488)
        self.hash = self.getHash()


class Blockchain:

    def __init__(self):
        self.chain = [Block("1488")]
        self.difficulty = 1
        self.blockTime = 30000

    def getLastBlock(self):
        return self.chain[len(self.chain) - 1]

    def addBlock(self, block):
        block.prevHash = self.getLastBlock().hash
        block.hash = block.getHash()
        block.mine(self.difficulty)
        self.chain.append(block)

        self.difficulty += (-1, 1)[int(time.time()) - int(self.getLastBlock().timestamp) < self.blockTime]

    def isValid(self):
        for i in range(1, len(self.chain)):
            currentBlock = self.chain[i]
            prevBlock = self.chain[i - 1]

            if (currentBlock.hash != currentBlock.getHash() or prevBlock.hash != currentBlock.prevHash):
                return False

        return True

    def __repr__(self):
        return json.dumps([{'data': item.data, 'timestamp': item.timestamp, 'nonce': item.nonce, 'hash': item.hash,
                            'prevHash': item.prevHash} for item in self.chain], indent=4)


class Node:
    def __init__(self):
        self.alive = 1
        self.vote = 1
        self.privateKey = PrivateKey()
        self.publicKey = self.privateKey.publicKey().toString()
        self.publicKeys = {}
        self.update = 0
        self.voteList = [1]
        self.id = self.getId()
        # print(self.id)
        self.leader = self.getLeaderId()
        self.Chain = Blockchain()
        self.chainLeng = len(self.Chain.chain)
        self.maxTimer = random.randrange(30, 50)
        self.timer = self.maxTimer
        self.clas = "leader" if self.id == self.leader else "follower"
        try:
            os.remove('node' + str(self.id) + '.db')
        except Exception:
            pass
        self.connection = sqlite3.connect('node' + str(self.id) + '.db', check_same_thread=False)
        print('node' + str(self.id) + '.db')
        self.cursor = self.connection.cursor()
        self.cursor.execute(
            "CREATE TABLE pocket (name TEXT PRIMARY KEY,  count INT)")
        self.connection.commit()
        global nodes
        nodes.append(self.id)

    def start(self):
        self.inpThread = threading.Thread(target=self.inputTrans)
        self.inpThread.start()
        self.candTimerThread = threading.Thread(target=self.candTimer)
        self.candTimerThread.start()
        if self.leader == self.id:
            # self.heartBeatThread = threading.Thread(target=self.heartBeat)
            # self.heartBeatThread.start()
            self.operPull = []
            self.operPullTread = threading.Thread(target=self.operToChainToDB)
            self.operPullTread.start()
            self.serverThraed = threading.Thread(target=self.server)
            self.serverThraed.start()
        # self.startVote()

    def inputTrans(self):
        while True:
            inp = input()
            if len(inp.split()) != 1:
                self.request(inp.split())
            if inp == "chain":
                print(self.Chain)
            if inp == "db":
                self.cursor.execute("SELECT * FROM pocket")
                chels = self.cursor.fetchall()
                print(list(chels))

    def server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # print(self.id, self.leader)
        sock.bind(("", 1488))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.listen(10)
        while True:
            try:
                # print(0)
                client, addr = sock.accept()
                # print(0)
                # client.setblocking(False)
                # print(client)
                # print(123)
                data = json.loads(client.recv(1024).decode("utf-8"))
                # print(123)
                # #print("Message: ", result.decode("utf-8"), addr)
                print(data)
                global nodes
                if data == "newId":
                    id = 0
                    for i in range(1488):
                        if i in nodes:
                            pass
                        else:
                            id = i
                            nodes.append(id)
                            break
                    # print(id)
                    client.send(json.dumps(nodes).encode("utf-8"))
                    client.send(json.dumps(id).encode("utf-8"))
                if data[0] == "pubKey":
                    #   print(1233)
                    self.publicKeys[data[1]] = data[2]
                    #  print(12333)
                    client.send(json.dumps(self.publicKeys).encode("utf-8"))
                # print(123333)
                if data == "leaderId":
                    client.send(json.dumps(self.id).encode("utf-8"))
                if data == "ping":
                    client.send(json.dumps(self.id).encode("utf-8"))
                    client.send(json.dumps(nodes).encode("utf-8"))
                    print(self.Chain)
                    tmp = []
                    for i in self.Chain.chain:
                        if i.data:
                            tmp.append({"from": i.data["from"], "to": i.data["to"], "amount": i.data["amount"],
                                        "timestamp": i.timestamp, "nou": i.nonce, "H": i.hash})

                    #print(json.dumps(tmp))
                    # print("TMP")
                    # print(tmp)
                    client.send(json.dumps(tmp).encode("utf-8"))
                    client.send(json.dumps(self.publicKeys).encode("utf-8"))
                    # print(1499)

                if data == "vote":
                    if self.leader == self.id:
                        client.send(json.dumps(self.id).encode("utf-8"))
                    else:
                        client.send(json.dumps(-1).encode("utf-8"))
                    self.voteList.append(1)
                if data[0] == "request":
                    # print(data[1])
                    data[1]["cli"] = client
                    # print(data[1])
                    self.operPull.append(data[1])
                if data == "check":
                    tmp = random.randint(0, 1488)
                    # print(tmp)
                    client.send(
                        json.dumps([str(tmp), Ecdsa.sign(str(tmp), self.privateKey).toBase64()]).encode("utf-8"))
                    # print(tmp)


            except Exception as fuck:
                print("fuck^", fuck)
                pass

    def canVote(self):
        self.vote = 0
        time.sleep(1)
        self.vote = 1

    def startVote(self):
        print("vopitinggg", self.id)
        if self.leader != self.id:
            self.clas = "candidate"
            try:
                self.serverThraed = threading.Thread(target=self.server)
                self.serverThraed.start()
            except Exception:
                # print(141010)
                self.clas = "follower"
                return
            global nodes
            #         tmp = []
            #          for i in nodes:
            #               tmp.append(i.ansVote())
            #            #print(tmp)
            # time.sleep(6)
            if self.voteList.count(1) >= 1:
                self.leader = self.id
                self.clas = "leader"
                print("IMLEADER")
                self.operPull = []
                self.operPullTread = threading.Thread(target=self.operToChainToDB)
                self.operPullTread.start()
            else:
                self.clas = "follower"

    def operToChainToDB(self):
        while self.leader == self.id and self.alive:
            time.sleep(5)
            i = 0
            while i < len(self.operPull):
                print(self.operPull)
                if self.operPull[i]["from"] == "None":
                    self.operPull[i]["from"] = None
                    # print(1488)
                if self.operPull[i]["to"] == None or self.operPull[i]["amount"] == 0:
                    # send error to self.operPull[i]["node"]

                    print(self.operPull[i]["node"], "send wrong request")
                    self.operPull.pop(i)
                else:
                    """if self.operPull[i]["from"] == None:
                        self.cursor.execute(("SELECT * FROM pocket WHERE name=?", (self.operPull[i]["to"])))
                        chel = self.cursor.fetchone()
                        if chel != None:
                            chel["amount"] += self.operPull[i]["from"]"""
                    if self.operPull[i]["from"] == None:
                        self.Chain.addBlock(Block(str(int(time.time())), (
                            {"from": None, "to": self.operPull[i]["to"], "amount": self.operPull[i]["amount"]})))
                        #   print(self.operPull[i]["to"])
                        self.cursor.execute("SELECT * FROM pocket WHERE name=?", (self.operPull[i]["to"],))
                        chel = self.cursor.fetchone()
                        if chel != None:
                            print(chel)
                            chel = list(chel)
                            #      chel = list(chel)
                            chel[1] += self.operPull[i]["amount"]
                            #     print(chel[1], self.operPull[i]["amount"])
                            self.cursor.execute("REPLACE INTO pocket VALUES(?, ?);", [chel[0], chel[1]])
                            self.connection.commit()
                            # chel[1] += self.operPull[i]["from"]
                        else:
                            self.cursor.execute("INSERT OR REPLACE INTO pocket VALUES(?, ?);",
                                                [self.operPull[i]["to"], self.operPull[i]["amount"]])
                            self.connection.commit()
                        print(self.Chain)
                        self.operPull.pop(i)
                    else:
                        self.cursor.execute("SELECT * FROM pocket WHERE name=?", (self.operPull[i]["from"],))
                        chel = self.cursor.fetchone()
                        if chel != None:
                            chel = list(chel)
                            if chel[1] >= self.operPull[i]["amount"]:
                                chel[1] -= self.operPull[i]["amount"]
                                self.cursor.execute("REPLACE INTO pocket VALUES(?, ?);", [chel[0], chel[1]])
                                self.connection.commit()
                                self.cursor.execute("SELECT * FROM pocket WHERE name=?", (self.operPull[i]["to"],))
                                chel = self.cursor.fetchone()
                                if chel != None:
                                    chel = list(chel)
                                    chel[1] += self.operPull[i]["amount"]
                                    # #print(chel[1], self.operPull[i]["amount"])
                                    self.cursor.execute("REPLACE INTO pocket VALUES(?, ?);", [chel[0], chel[1]])
                                    self.connection.commit()
                                    self.Chain.addBlock(Block(str(int(time.time())), (
                                        {"from": self.operPull[i]["from"], "to": self.operPull[i]["to"],
                                         "amount": self.operPull[i]["amount"]})))
                                    self.operPull.pop(i)
                                else:
                                    self.Chain.addBlock(Block(str(int(time.time())), (
                                        {"from": self.operPull[i]["from"], "to": self.operPull[i]["to"],
                                         "amount": self.operPull[i]["amount"]})))
                                    # #print(self.operPull[i]["to"])
                                    self.cursor.execute("INSERT OR REPLACE INTO pocket VALUES(?, ?);",
                                                        [self.operPull[i]["to"], self.operPull[i]["amount"]])
                                    self.connection.commit()

                                    self.operPull.pop(i)
                            else:
                                # print(self.operPull[i]["node"], "send wrong request")
                                self.operPull.pop(i)
                        else:
                            # print(self.operPull[i]["node"], "send wrong request")
                            self.operPull.pop(i)
            if self.chainLeng != len(self.Chain.chain):
                self.chainLeng = len(self.Chain.chain)
                self.update = 1
                # print(nodes)
                # for i in nodes:
                #    i.Chain = self.Chain
                #    i.rebuildDB()

    def rebuildDB(self):
        for i in range(self.chainLeng, len(self.Chain.chain)):
            block = self.Chain.chain[i]
            # print(block)
            # print(block.data)
            fro = block.data.get("from")
            to = block.data.get('to')
            amount = block.data.get('amount')
            print(fro, to, amount)
            if fro != None:
                self.cursor.execute("SELECT * FROM pocket WHERE name=?", (fro,))
                chel = self.cursor.fetchone()
                chel = list(chel)
                chel[1] -= amount
                self.cursor.execute("REPLACE INTO pocket VALUES(?, ?);", [chel[0], chel[1]])
                self.connection.commit()

            self.cursor.execute("SELECT * FROM pocket WHERE name=?", (to,))
            chel = self.cursor.fetchone()
            if chel != None:
                # print(chel)
                chel = list(chel)
                chel[1] += amount
                # #print(chel[1], self.operPull[i]["amount"])
                self.cursor.execute("REPLACE INTO pocket VALUES(?, ?);", [chel[0], chel[1]])
                self.connection.commit()
                # chel["amount"] += self.operPull[i]["from"]
            else:
                self.cursor.execute("INSERT OR REPLACE INTO pocket VALUES(?, ?);",
                                    [to, amount])
                self.connection.commit()
        self.chainLeng = len(self.Chain.chain)
        # print(self.id, self.Chain)

    def ansVote(self):
        if self.vote:
            threading.Thread(target=self.canVote).start()
            return 1
        else:
            return 0

    def voteMe(self, node, cand):
        return node.ansVote(node, cand)

    def checkConnection(self, arr):
        return Ecdsa.verify(arr[0], Signature.fromBase64(arr[1]),
                            PublicKey.fromString(self.publicKeys[str(self.leader)]))

    def candTimer(self):
        while self.leader != self.id and self.alive:
            time.sleep(0.1)
            self.timer -= 1
            # #print(self.timer)
            if self.timer == 0:
                try:
                    global nodes
                    sock = socket.socket()
                    sock.connect(('localhost', 1488))
                    sock.send(json.dumps("check").encode("utf-8"))
                    check = json.loads(sock.recv(1024).decode("utf-8"))
                    #print(123123)
                    if not self.checkConnection(check):
                        print("Wrong connection!")
                    #print(321321)
                    sock.close()
                    sock = socket.socket()
                    sock.connect(('localhost', 1488))
                    sock.send(json.dumps("ping").encode("utf-8"))  # ??????
                    self.timer = self.maxTimer
                    leader = json.loads(sock.recv(1024).decode("utf-8"))
                    # print()
                    #print(1488)
                    nodes = json.loads(sock.recv(1024).decode("utf-8"))
                    #print(1488)
                    chain = json.loads(sock.recv(1024).decode("utf-8"))
                    #print(1488)
                    self.publicKeys = json.loads(sock.recv(1024).decode("utf-8"))
                    #print(1488)
                    #   print(self.publicKeys)
                    #print("chain")
                    # print(chain)
                    if chain != []:
                        flag = 0
                        for i in chain:
                            if not i["H"] in [i.hash for i in self.Chain.chain]:
                                flag = 1
                                self.Chain.addBlock(Block(str(int(time.time())), (
                                {"from": i["from"], "to": i["to"], "amount": int(i["amount"])})))
                                # print(0)
                                self.Chain.chain[-1].nonce = i["nou"]
                                self.Chain.chain[-1].timestamp = i["timestamp"]
                                self.Chain.chain[-1].hash = i["H"]
                        if flag:
                            # print(1)
                            self.rebuildDB()
                            # print(2)
                    # print(self.Chain)
                    # print(1488)
                    # print(1488)
                    sock.close()
                    if leader != self.leader:
                        sock = socket.socket()
                        sock.connect(('localhost', 1488))
                        sock.send(json.dumps("vote").encode("utf-8"))
                        # print(1499)
                        check = json.loads(sock.recv(1024).decode("utf-8"))
                        sock.close()
                        if check != -1:
                            self.leader = check
                    # print(1488)
                    continue
                except Exception as fuck:
                    print("fuck", fuck)
                    self.startVote()
                    self.timer = self.maxTimer

    # def heartBeat(self):
    #   while self.leader == self.id and self.alive:
    #      for i in nodes:
    #         if i != self:
    #            i.timer = i.maxTimer
    #   time.sleep(1)

    def getId(self):
        try:
            sock = socket.socket()
            sock.connect(('localhost', 1488))
            # print(1488)
            # print(self.publicKey)
            sock.send(json.dumps("newId").encode("utf-8"))
            # print(1499)
            nodeList = json.loads(sock.recv(1024).decode("utf-8"))
            # print(nodeList)
            global nodes
            nodes = nodeList
            # print(123)
            self.id = json.loads(sock.recv(1024).decode("utf-8"))
            # print("ID = ", self.id)
            # print(321)
            sock.close()
            sock = socket.socket()
            sock.connect(('localhost', 1488))
            sock.send(json.dumps(["pubKey", str(self.id), self.publicKey]).encode("utf-8"))
            # print(123)
            self.publicKeys = json.loads(sock.recv(1024).decode("utf-8"))
            # print(self.publicKeys)
            sock.close()
            # print(321)
            # print(self.id)
            return self.id
        except Exception as fuck:
            # print("fuck^", fuck)
            self.publicKeys["0"] = self.publicKey
            return 0
        """global nodes
        if len(nodes) == 0:
            return 0
        for i in range(1488):
            if not i in [j.id for j in nodes]:
                return i"""

    def isLeader(self):
        if self.leader == self.id:
            return True
        else:
            return False

    def getLeaderId(self):
        try:
            sock = socket.socket()
            sock.connect(('localhost', 1488))
            sock.send(json.dumps("leaderId").encode("utf-8"))
            leaderid = json.loads(sock.recv(1024).decode("utf-8"))
            sock.close()
            return leaderid
        except Exception:
            return self.id

    def request(self, inp):
        if inp[0] == "None":
            inp[0] = None
        oper = {"from": inp[0], "to": inp[1], "amount": int(inp[2]), "node": self.id}
        print(oper)
        sock = socket.socket()
        sock.connect(('localhost', 1488))
        sock.send(json.dumps(["request", oper]).encode("utf-8"))
        sock.close()

        """for i in nodes:
            if i.id == self.leader:
                i.operPull.append(oper)"""

    def kill(self):
        global nodes
        for i in range(len(nodes)):
            if nodes[i] == self:
                nodes.pop(i)
            break
        self.alive = 0

    def __repr__(self):
        # print(self.Chain)
        return json.dumps({'class': self.clas, 'id': self.id, 'leader': self.leader, "time": self.maxTimer}, indent=4)


node0 = Node()
print(node0.id, node0)
# #print(nodes)
# threading.Thread(target=node0.start).start()
# .start()).start()
# node1 = Node()
# print(1, node1)
# time.sleep(1)
node0.start()
# node1.start()


# node0.request("Invoker", "Pudge", 1488)
# node1.request(None, "Invoker", 1488)
# node2.request("Invoker", "Pudge", 1488)
# node1.request("Invoker", "Pudge", 1488)

# node0.kill()
# time.sleep(7)
# print(node1)
# print(node2)
# node1.kill()
# time.sleep(7)
# print(node2)

print("1488 " * 10)
