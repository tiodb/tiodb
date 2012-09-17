using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Threading;
using System.Linq;

namespace InteliHubExplorer
{
    public partial class Explorer : Form
    {
        struct ContainerItem
        {
            public string key;
            public string value;
        }

        InteliHubClient.Connection m_connection;
        string m_server = (string)Application.UserAppDataRegistry.GetValue("server", "localhost");
        Int16 m_port = Convert.ToInt16(Application.UserAppDataRegistry.GetValue("port", 2605));
        string m_containerName = (string)Application.UserAppDataRegistry.GetValue("containerName", "");
        Thread m_listingThread = null;
        int m_containerCount = 0;
        bool m_stopping = false;

        string m_currentFilterExpression = "";
        string m_newFilterExpression = "";

        List<ContainerItem> m_containerList = new List<ContainerItem>();
        List<ContainerItem> m_filteredItems = null;

        public Explorer()
        {
            InitializeComponent();

            serverTextBox.Text = m_server;
            portTextBox.Text = m_port.ToString();
            containerNameTextBox.Text = m_containerName;
        }

        private void connectButton_Click(object sender, EventArgs e)
        {
            if (m_connection != null)
                m_connection.Disconnect();

            m_connection = new InteliHubClient.Connection(m_server, m_port);

            Application.UserAppDataRegistry.SetValue("server", m_server);
            Application.UserAppDataRegistry.SetValue("port", m_port);

            statusLabel.Text = "Connected! Click \"update list\" to download container list";

            if (m_server == "localhost" || m_server == "localhost")
            {
                LoadContainerList();
                filterTextBox.Focus();
            }
        }

        private void LoadContainerList()
        {
            if (m_listingThread != null && m_listingThread.ThreadState == ThreadState.Running)
                return;

            m_containerCount = m_connection.Open("meta/containers").Count;

            containersListView.Items.Clear();
            m_containerList.Clear();

            m_listingThread = new Thread(delegate()
            {
                m_connection.Open("meta/containers").Query(
                    delegate(object key, object value, object metadata)
                    {
                        if (m_stopping)
                        {
                            m_connection.Close();
                            return;
                        }

                        lock (m_containerList)
                        {
                            m_containerList.Add(new ContainerItem { key=key.ToString(), value=value.ToString() });
                        }
                    });
            });

            m_listingThread.Start();
        }

        

        private void Explorer_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F5)
            {
                LoadContainerList();
                filterTextBox.Focus();
            }
        }

        private void containersListView_DoubleClick(object sender, EventArgs e)
        {
            new ContainerViewer(m_server, m_port, containersListView.SelectedItems[0].Text).Show();
        }

        private void containersListView_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                OpenListViewSelectedContainer();
            }
        }

        private void serverTextBox_TextChanged(object sender, EventArgs e)
        {
            m_server = serverTextBox.Text;
        }

        private void portTextBox_TextChanged(object sender, EventArgs e)
        {
            try
            {
                m_port = Convert.ToInt16(portTextBox.Text);
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.ToString());
            }
        }

        private void btnOpenContainer_Click(object sender, EventArgs e)
        {
            OpenContainer(containerNameTextBox.Text);
        }

        void OpenListViewSelectedContainer()
        {
            OpenContainer(containersListView.SelectedItems[0].Text);
        }

        void OpenContainer(string containerName)
        {
            m_containerName = containerNameTextBox.Text = containerName;
            Application.UserAppDataRegistry.SetValue("containerName", m_containerName);
            new ContainerViewer(m_server, m_port, m_containerName).Show();
        }

        private void Explorer_Load(object sender, EventArgs e)
        {

        }

        private void updateContainerListButton_Click(object sender, EventArgs e)
        {
            LoadContainerList();
            filterTextBox.Focus();
            updateContainerListButton.Enabled = false;
        }

        void UpdateContainerListView()
        {
            try
            {
                ApplyFilter();

                List<ContainerItem> listToUse = null;

                if (m_filteredItems != null)
                    listToUse = m_filteredItems;
                else
                    listToUse = m_containerList;

                ListViewItem[] toBeAdded = null;

                lock (m_containerList)
                {
                    if (listToUse.Count > containersListView.Items.Count)
                    {
                        int toBeAddedCount = listToUse.Count - containersListView.Items.Count;
                        toBeAdded = new ListViewItem[toBeAddedCount];

                        int b = 0;
                        for (int a = containersListView.Items.Count; a < listToUse.Count; a++)
                        {
                            ContainerItem item = listToUse[a];
                            toBeAdded[b] = new ListViewItem(new string[] { item.key, item.value });
                            b++;
                        }
                    }
                }

                if (toBeAdded != null)
                {
                    containersListView.Items.AddRange(toBeAdded);
                }
            }
            finally
            {
                //Cursor = Cursors.Default;
            }

            containerCountLabel.Text = String.Format("{0}/{1} containers {2}",
                containersListView.Items.Count,
                m_containerCount,
                m_filteredItems != null ? "(filtered)" : "");
        }

        private void updateListViewTimer_Tick(object sender, EventArgs e)
        {
            UpdateContainerListView();
        }

        void ApplyFilter()
        {
            //if (m_currentFilterExpression == m_newFilterExpression)
            //    return;

            if (String.IsNullOrWhiteSpace(m_newFilterExpression))
            {
                if (m_currentFilterExpression != m_newFilterExpression)
                {
                    Cursor = Cursors.WaitCursor;
                    containersListView.Items.Clear();
                    Cursor = Cursors.Default;
                }

                m_filteredItems = null;
                m_currentFilterExpression = m_newFilterExpression;
                return;
            }

            lock (m_containerList)
            {
                m_filteredItems = (from item in m_containerList
                                   where item.key.Contains(m_newFilterExpression)
                                   select item).ToList();
            }

            if (m_currentFilterExpression != m_newFilterExpression)
            {
                Cursor = Cursors.WaitCursor;
                containersListView.Items.Clear();
                Cursor = Cursors.Default;
            }

            m_currentFilterExpression = m_newFilterExpression;
        }

        private void filterButton_Click(object sender, EventArgs e)
        {
            m_newFilterExpression = filterTextBox.Text;
            UpdateContainerListView();
        }

        private void filterTextBox_TextChanged(object sender, EventArgs e)
        {
            //m_newFilterExpression = filterTextBox.Text;
            //UpdateContainerListView();
        }

        private void Explorer_FormClosing(object sender, FormClosingEventArgs e)
        {
            m_stopping = true;
        }

        private void filterTextBox_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                m_newFilterExpression = filterTextBox.Text;
                UpdateContainerListView();
            }
        }
    }
}
