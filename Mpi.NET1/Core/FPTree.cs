using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Mpi.NET1.Core
{
    public class Node
    {
        #region Properties
        public int itemName = -1;
        public int count;
        public Node[] nodeChildrens = new Node[10];
        public Node nodeChild;
        public Node nodeParent;
        public Node nodeLink;// liên kết các node có item giống nó trên cây
        public bool visited;
        #endregion
        #region Methods
        public Node CreateNode(int name)
        {
            Node node = new Node();
            node.itemName = name;
            node.count = 0;
            node.nodeChildrens = new Node[10];
            node.nodeChild = null;
            node.nodeParent = null;
            node.nodeLink = null;
            node.visited = false;
            return node;
        }
        #endregion
    }
    public class List
    {
        #region Properties
        public Node pHead;
        public Node pTail;
        #endregion
        #region Methods
        public List CreateList()
        {
            List list = new List();
            list.pHead = null;
            list.pTail = null;
            return list;
        }

        public List InsertTail(List list, Node node)
        {
            if (list.pHead == null)
            {
                list.pHead = node;
                list.pTail = list.pHead;
            }
            else
            {
                node.nodeParent = list.pTail;
                list.pTail.nodeChild = node;
                list.pTail = node;
            }
            return list;
        }
        #endregion
    }
    public class FPTree
    {
        #region Properties
        public Node root = new Node();
        public List<Node> arrayNode = new List<Node>();
        //public Node[] arrayNode = new Node[999999];
        public int countNode;

        #endregion

        #region Methods
        public FPTree CreateTree()
        {
            FPTree tree = new FPTree();
            tree.root = new Node();
            tree.arrayNode = new List<Node>(); //new Node[999999];
            tree.countNode = 0;
            return tree;
        }
        public FPTree InsertNode(FPTree tree, List list)
        {
            int i = 0;
            Node root = new Node();
            root = tree.root;
            Node node = new Node();
            node = list.pHead;
            while (node != null)
            {
                bool flag = false;
                // tìm rtrong root có itemName nào trùng với node.itemName không? 
                for (i = 0; i < root.nodeChildrens.Length; i++)
                {
                    if (root.nodeChildrens[i] != null)
                        if (root.nodeChildrens[i].itemName == node.itemName)
                        {
                            flag = true;
                            break;
                        }
                }
                // nếu tìm thấy thì tăng cout của itemName đó lên 1
                if (flag == true)
                {
                    root.nodeChildrens[i].count += 1;
                    if (node.nodeChild != null && node.nodeChild.nodeParent != null)
                    {
                        node.nodeChild.nodeParent = root.nodeChildrens[i];
                    }

                }
                // ngược lại tạo node mới
                else
                {
                    node.nodeParent = root;
                    node.count += 1;
                    tree.countNode += 1;
                    // gán node là con của root
                    for (i = 0; i < root.nodeChildrens.Length; i++)
                    {
                        if (root.nodeChildrens[i] == null)
                        {
                            root.nodeChildrens[i] = node;
                            break;
                        }
                    }
                    tree.arrayNode.Add(node);
                  
                }
                if (node.nodeChild == null)
                {
                    root = node;
                }
                else
                {
                    root = node.nodeChild.nodeParent;
                }
                node = node.nodeChild;
            }
            return tree;
        }
        #endregion
    }
}
