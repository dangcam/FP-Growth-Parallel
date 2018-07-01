using Mpi.NET1.Core;
using MPI;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mpi.NET1
{
    class Program
    {
        //Install-Package MPI.NET -Version 1.3.0

        // MPIEXEC -n 3 Mpi.NET1.exe "data3.txt" "50"
        static void Main(string[] args)
        {

            using (MPI.Environment environment = new MPI.Environment(ref args))
            {
                if (args.Length < 2)
                {
                    Console.WriteLine("At least two Arguments are needed");
                    return;
                }

                Intracommunicator comm = MPI.Communicator.world;

                if (comm.Size < 2)
                {
                    Console.WriteLine("At least two processes are needed");
                    return;
                }

                float support = float.Parse(args[1]);
                if (comm.Rank == 0) // It's the root
                {
                    // đọc dữ liệu đầu vào
                    string inputFile = args[0];
                    string[] database = System.IO.File.ReadAllLines(inputFile);
                    List<List<int>> db = new List<List<int>>();
                    List<int> items;
                    foreach (string item in database)
                    {
                        items = new List<int>();
                        foreach (string it in item.Split(','))
                        {
                            items.Add(int.Parse(it));
                        }
                        db.Add(items);
                    }

                    for (int i = 1; i < comm.Size; i++)
                    {
                        comm.Send(db, i, 0);// gửi tới các tiến trình i, tag 0
                    }
                    Dictionary<int, int> tongHop = new Dictionary<int, int>();
                    for (int i = 1; i < comm.Size; i++)
                    {
                        Dictionary<int, int> demItem;
                        comm.Receive(i, 1, out demItem);// nhận từ i, tag 1
                        foreach (var item in demItem)
                        {
                            if (tongHop.ContainsKey(item.Key))
                            {
                                tongHop[item.Key] += item.Value;
                            }
                            else
                            {
                                tongHop.Add(item.Key, item.Value);
                            }
                        }
                    }
                    int len = db.Count;
                    //var ordered = tongHop.OrderByDescending(x => x.Value);
                    List<int> keys = new List<int>(tongHop.Keys);
                    foreach (var key in keys)
                    {
                        //Console.WriteLine("Item {0}", key);
                        if (((float)tongHop[key] / (float)len * 100.0) < support)
                        {
                            tongHop.Remove(key);
                        }
                    }
                    tongHop = tongHop.OrderByDescending(x => x.Value).ToDictionary(x => x.Key, y => y.Value);
                    //foreach (var item in tongHop)
                    //{
                    //    Console.WriteLine("Item {0} count {1}", item.Key, item.Value);
                    //}
                    //tongHop.OrderByDescending(x => x.Value)
                    // gửi 1-item phổ biến
                    for (int i = 1; i < comm.Size; i++)
                    {
                        comm.Send(tongHop, i, 2);// gửi tới các tiến trình i, tag 2
                    }
                    //
                    Dictionary<int, Dictionary<int, Dictionary<int, int>>> Pall =
                        new Dictionary<int, Dictionary<int, Dictionary<int, int>>>();
                    for (int i = 1; i < comm.Size; i++)
                    {
                        Dictionary<int, Dictionary<int, Dictionary<int, int>>> P;
                        comm.Receive(i, 3, out P);// nhận từ i, tag 1
                        foreach (var itemset in P)
                        {
                            if (Pall.ContainsKey(itemset.Key))
                            {
                                foreach (var item in itemset.Value)
                                {
                                    Dictionary<int, Dictionary<int, int>> keyValues = Pall[itemset.Key];
                                    if (keyValues.ContainsKey(item.Key))
                                    {
                                        Dictionary<int, int> kv = keyValues[item.Key];
                                        //keyValues[item.Key] += item.Value;
                                        foreach (var k in item.Value)
                                        {
                                            if (kv.ContainsKey(k.Key))
                                                kv[k.Key] += k.Value;
                                            else
                                                kv.Add(k.Key, k.Value);
                                        }
                                    }
                                    else
                                    {
                                        keyValues.Add(item.Key, item.Value);
                                    }
                                    Pall[itemset.Key] = keyValues;
                                }
                            }
                            else
                            {
                                Pall.Add(itemset.Key, itemset.Value);
                            }
                        }
                    }

                    List<int> itemkey = new List<int>(Pall.Keys);
                    foreach (var itemset in itemkey)
                    {
                        Dictionary<int, Dictionary<int, int>> keyValues = Pall[itemset];
                        Dictionary<int, int> coutValue = CountValue(keyValues);
                        //PrintDictionary(coutValue);
                        List<int> kv = new List<int>(keyValues.Keys);
                        foreach (var item in kv)
                        {
                            Dictionary<int, int> keyValue = keyValues[item];
                            List<int> ikey = new List<int>(keyValue.Keys);
                            foreach (var k in ikey)
                                if ((float)coutValue[k] / len * 100.0 < support)
                                    keyValue.Remove(k);
                            keyValues[item] = keyValue;
                        }
                        //keyValues.Add(itemset, tongHop[itemset]);
                        Pall[itemset] = keyValues;
                    }
                    //foreach (var itemset in Pall)
                    //{
                    //    Console.WriteLine("FPTree: {0}", itemset.Key);
                    //    foreach (var item in itemset.Value)
                    //    {
                    //        Console.WriteLine("Node: {0}", item.Key);
                    //        foreach (var key in item.Value)
                    //            Console.Write("Item {0} count {1} -> ", key.Key, key.Value);
                    //        Console.WriteLine();
                    //    }
                    //    Console.WriteLine();
                    //}
                    //foreach (var itemset in Pall)
                    //{
                    //    Console.WriteLine("Item {0}: ", itemset.Key);
                    //    foreach (var item in itemset.Value)
                    //        Console.WriteLine("Item {0} count {1}", item.Key, item.Value);
                    //}
                    Dictionary<List<int>, float> FPTreeCon = new Dictionary<List<int>, float>();
                    foreach (var itemset in Pall)
                    {
                        FPTreeCon.Add(new List<int> { itemset.Key }, (float)tongHop[itemset.Key] / len * 100f);
                        foreach (var item in itemset.Value)
                        {
                            List<int> keyValues = new List<int>(item.Value.Keys);
                            //Console.WriteLine("keyValues {0}", string.Join(",", keyValues.ToArray()));
                            List<List<int>> subsets = Bit.FindSubsets(keyValues, 0); //get all subsets    
                            foreach (var itms in subsets)
                            {
                                if (itms.Count > 0)
                                {
                                    //Console.WriteLine("subsets {0}", string.Join(",", itms.ToArray()));
                                    float itemsupport = FindSupport(len, Pall[itemset.Key], itms);
                                    itms.Add(itemset.Key);
                                    if (itemsupport >= support && !ContainsKey(new List<List<int>>(FPTreeCon.Keys), itms))
                                        FPTreeCon.Add(itms, itemsupport);
                                }
                            }
                        }
                    }
                    //foreach (var itemset in FPTreeCon)
                    //{
                    //    Console.WriteLine("Item {0}  - support {1}", String.Join(", ", itemset.Key.ToArray()), itemset.Value);
                    //}
                    using (StreamWriter outputFile = new StreamWriter("OutputFPGrowth.txt"))
                    {
                        foreach (var itemset in FPTreeCon)
                            outputFile.WriteLine("{0}:{1}", string.Join(",", itemset.Key.ToArray()), itemset.Value);
                    }
                }
                else
                {
                    List<List<int>> db;
                    comm.Receive(0, 0, out db);// nhận từ 0, tag 0
                    Dictionary<int, int> demItem = new Dictionary<int, int>();
                    // tính chia db
                    int size = comm.Size - 1;
                    int len = db.Count;
                    int n = len / size;
                    int start = (comm.Rank - 1) * n;
                    int end = comm.Rank * n;
                    if (comm.Rank == size)
                    {
                        end = len;
                    }
                    //
                    for (int i = start; i < end; i++)
                    {
                        foreach (int item in db[i])
                            if (demItem.ContainsKey(item))
                            {
                                demItem[item]++;
                            }
                            else
                            {
                                demItem.Add(item, 1);
                            }
                    }
                    // item cout cục bộ
                    comm.Send(demItem, 0, 1);// gửi tới các tiến trình 0, tag 1
                    // nhận 1-itemset phổ biến
                    Dictionary<int, int> Fre;
                    comm.Receive(0, 2, out Fre);// nhận từ 0, tag 2

                    // T itemset cục bộ
                    List<List<int>> Tcucbo = new List<List<int>>();
                    for (int i = start; i < end; i++)
                    {
                        List<int> itemset = new List<int>();
                        foreach (var item in Fre)
                            if (db[i].Contains(item.Key))
                            {
                                itemset.Add(item.Key);
                            }
                        if (itemset.Count > 0)
                            Tcucbo.Add(itemset);
                    }
                    //if(comm.Rank == 2)
                    //foreach (var items in Tcucbo)
                    //{
                    //    foreach (var item in items)
                    //        Console.Write(item + " ");
                    //    Console.WriteLine();
                    //}
                    // xây dựng FP-Tree cục bộ
                    FPTree tree = CreateTree(Tcucbo);
                    // Conditional Patern Bases
                    Dictionary<int, Dictionary<int, Dictionary<int, int>>>
                        P = new Dictionary<int, Dictionary<int, Dictionary<int, int>>>();
                    foreach (var itemset in Fre)
                    {
                        Dictionary<int, Dictionary<int, int>> nodeParent = new Dictionary<int, Dictionary<int, int>>();
                        //List<int> itemsetCollectionKey = new List<int>();
                        //int itemsetCollectionValue = 0;
                        //Console.WriteLine("Node: {0}", itemset.Key);
                        for (int j = 0; j < tree.countNode; j++)
                        {
                            Dictionary<int, int> itemsetCollection = new Dictionary<int, int>();
                            var node = tree.arrayNode[j];
                            if (node.itemName == itemset.Key && !node.visited)
                            {
                                node.visited = true;
                                var nodeparent = node.nodeParent;
                                while (nodeparent.itemName > -1)
                                {
                                    //bool index = itemsetCollection.Key.Contains(nodeparent.itemName);
                                    if (itemsetCollection.ContainsKey(nodeparent.itemName))
                                    {
                                        itemsetCollection[nodeparent.itemName] += node.count;
                                    }
                                    else
                                    {
                                        itemsetCollection.Add(nodeparent.itemName, node.count);
                                    }
                                    //Console.Write("Item:{0}({1})->", nodeparent.itemName, node.count);
                                    nodeparent = nodeparent.nodeParent;
                                }
                                if (itemsetCollection.Count > 0)
                                {
                                    if (nodeParent.ContainsKey(itemsetCollection.Keys.Last()))
                                    {
                                        Dictionary<int, int> keyValues = nodeParent[itemsetCollection.Keys.Last()];
                                        foreach (var item in itemsetCollection)
                                        {
                                            if (keyValues.ContainsKey(item.Key))
                                            {
                                                keyValues[item.Key] += item.Value;
                                            }
                                            else
                                            {
                                                keyValues.Add(item.Key, item.Value);
                                            }
                                        }
                                        nodeParent[itemsetCollection.Keys.Last()] = keyValues;
                                    }
                                    else
                                    {
                                        nodeParent.Add(itemsetCollection.Keys.Last(), itemsetCollection);
                                    }
                                }
                            }
                        }
                        P.Add(itemset.Key, nodeParent);
                    }
                    comm.Send(P, 0, 3);// gửi tới các tiến trình 0, tag 3
                }
            }
        }
        private static bool ContainsKey(List<List<int>> keys, List<int> items)
        {
            items.Sort();
            foreach (var key in keys)
            {
                key.Sort();
                if(string.Join("", key.ToArray()) == string.Join("", items.ToArray()))
                    return true;
            }
            return false;
        }
        private static Dictionary<int,int> CountValue(Dictionary<int, Dictionary<int,int>> keyValues)
        {
            Dictionary<int, int> cout = new Dictionary<int, int>();
            foreach(var keyvalue in keyValues)
            {
                foreach(var kv in keyvalue.Value)
                {
                    if (cout.ContainsKey(kv.Key))
                        cout[kv.Key] += kv.Value;
                    else
                        cout.Add(kv.Key, kv.Value);
                }
            }
            return cout;
        }
        private static void PrintDictionary(Dictionary<int, int> keyValuePairs)
        {
            foreach (var kv in keyValuePairs)
            {
                Console.WriteLine("Item {0} count {1} ", kv.Key, kv.Value);
            }
        }
        private static float FindSupport(int len, Dictionary<int, Dictionary<int, int>> keyValuePairs, List<int> items)
        {
            //if (items.Count == 0)
            //    return (float)value / len * 100f;
            float support = 0;
            int min = 0;
            foreach (var kv in keyValuePairs)
            {
                if (kv.Value.Count > 0)
                {
                    Dictionary<int, int> kvalue = kv.Value;
                    try
                    {
                        min += items.Select(k => kvalue[k]).Min();
                    }
                    catch { }
                }
            }
            support = (float)min / len * 100f;
            return support;
        }
        private static FPTree CreateTree(List<List<int>> T)
        {
            FPTree tree = new FPTree();
            List[] list_frequencyItems_TID;

            int i = 0;
            list_frequencyItems_TID = ToList(T);
            for (i = 0; i < T.Count; i++)
            {
                List list = new List();
                list = list_frequencyItems_TID[i];
                tree = tree.InsertNode(tree, list);
            }
            return tree;
        }
        private static List[] ToList(List<List<int>> frequencyItemsTID)
        {
            List[] mangList = new List[frequencyItemsTID.Count];
            int i = 0, j = 0;

            for (i = 0; i < frequencyItemsTID.Count; i++)
            {
                List list = new List();
                list = list.CreateList();
                Node node = new Node();
                //Tao List cho TID
                if (frequencyItemsTID[i] != null)
                    for (j = 0; j < frequencyItemsTID[i].Count; j++)
                    {
                        node = node.CreateNode(frequencyItemsTID[i][j]);
                        list = list.InsertTail(list, node);
                    }
                mangList[i] = list;
            }
            return mangList;
        }
    }
}
