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
        // MPIEXEC -n 3 Mpi.NET1.exe "data3.txt" "50" "70"
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
                string inputFile = args[0];
                float support = float.Parse(args[1]);
                if (comm.Rank == 0) // It's the root
                {
                    // đọc dữ liệu đầu vào

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
                    Dictionary<int, Dictionary<int, int>> Pall = new Dictionary<int, Dictionary<int, int>>();
                    for (int i = 1; i < comm.Size; i++)
                    {
                        Dictionary<int, Dictionary<int, int>> P;
                        comm.Receive(i, 3, out P);// nhận từ i, tag 1
                        foreach (var itemset in P)
                        {
                            if (Pall.ContainsKey(itemset.Key))
                            {
                                foreach (var item in itemset.Value)
                                {
                                    Dictionary<int, int> keyValues = Pall[itemset.Key];
                                    if (keyValues.ContainsKey(item.Key))
                                    {
                                        keyValues[item.Key] += item.Value;
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
                    //foreach (var itemset in Pall)
                    //{
                    //    Console.WriteLine("Item {0}: ", itemset.Key);
                    //    foreach (var item in itemset.Value)
                    //        Console.WriteLine("Item {0} count {1}", item.Key, item.Value);
                    //}
                    List<int> itemkey = new List<int>(Pall.Keys);
                    foreach (var itemset in itemkey)
                    {
                        Dictionary<int, int> keyValues = Pall[itemset];
                        List<int> kv = new List<int>(keyValues.Keys);
                        foreach (var item in kv)
                        {
                            if ((float)keyValues[item] / len * 100.0 < support)
                                keyValues.Remove(item);
                        }
                        //keyValues.Add(itemset, tongHop[itemset]);
                        Pall[itemset] = keyValues;
                    }

                    //foreach (var itemset in Pall)
                    //{
                    //    Console.WriteLine("Item {0}: ", itemset.Key);
                    //    foreach (var item in itemset.Value)
                    //        Console.WriteLine("Item {0} count {1}", item.Key, item.Value);
                    //}
                    Dictionary<List<int>, float> FPTreeCon = new Dictionary<List<int>, float>();
                    foreach (var itemset in Pall)
                    {
                        List< int> keyValues = new List<int>( Pall[itemset.Key].Keys);
                        List<List<int>> subsets = Bit.FindSubsets(keyValues, 0); //get all subsets
                        foreach(var itms in subsets)
                        {
                            float itemsupport = FindSupport(len, Pall[itemset.Key], itms, tongHop[itemset.Key]);
                            itms.Add(itemset.Key);
                            FPTreeCon.Add(itms, itemsupport);
                        }
                    }
                    //foreach (var itemset in FPTreeCon)
                    //{
                    //    Console.WriteLine("Item {0}  - support {1}", String.Join(", ", itemset.Key.ToArray()), itemset.Value);
                    //}
                    using (StreamWriter outputFile = new StreamWriter("Output.txt"))
                    {
                        foreach (var itemset in FPTreeCon)
                            outputFile.WriteLine("{0}:{1}", String.Join(",", itemset.Key.ToArray()), itemset.Value);
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
                    Dictionary<int, Dictionary<int, int>> P = new Dictionary<int, Dictionary<int, int>>();
                    foreach (var itemset in Fre)
                    {
                        Dictionary<int, int> itemsetCollection = new Dictionary<int, int>();
                        //List<int> itemsetCollectionKey = new List<int>();
                        //int itemsetCollectionValue = 0;
                        //Console.WriteLine("Node: {0}", itemset.Key);
                        for (int j = 0; j < tree.countNode; j++)
                        {
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
                            }
                        }
                        P.Add(itemset.Key, itemsetCollection);
                    }
                    comm.Send(P, 0, 3);// gửi tới các tiến trình 0, tag 3
                }
            }
        }
        private static float FindSupport(int len, Dictionary<int, int> keyValuePairs, List<int> items, int value)
        {
            if (items.Count == 0)
                return (float)value/len*100f;
            float support = 0;
            int min = items.Select(k => keyValuePairs[k]).Min();
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
