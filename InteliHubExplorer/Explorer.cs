using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using System.Threading;

namespace InteliHubExplorer
{
    public partial class Explorer : Form
    {
        InteliHubClient.Connection _connection;
        string m_server = (string)Application.UserAppDataRegistry.GetValue("server", "localhost");
        Int16 m_port = Convert.ToInt16(Application.UserAppDataRegistry.GetValue("port", 2605));
        bool m_listing = false;
        int m_containerCount = 0;

        Dictionary<string, string> m_toAddToListView = new Dictionary<string, string>();

        public Explorer()
        {
            InitializeComponent();

            serverTextBox.Text = m_server;
            portTextBox.Text = m_port.ToString();
        }

        private void connectButton_Click(object sender, EventArgs e)
        {
            if (_connection != null)
                _connection.Disconnect();

            _connection = new InteliHubClient.Connection(m_server, m_port);


            Application.UserAppDataRegistry.SetValue("server", m_server);
            Application.UserAppDataRegistry.SetValue("port", m_port);

            statusLabel.Text = "Connected! Click \"update list\" to download container list";
        }

        private void LoadContainerList()
        {
            if (m_listing)
                return;

            m_listing = true;


            m_containerCount = _connection.Open("meta/containers").Count;

            containersListView.Items.Clear();

            new Thread(delegate()
            {
                _connection.Open("meta/containers").Query(
                    delegate(object key, object value, object metadata)
                    {
                        lock (m_toAddToListView)
                        {
                            m_toAddToListView.Add(key.ToString(), value.ToString());
                        }
                    });

                m_listing = false;
            }).Start();
            
        }

        

        private void Explorer_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F5)
                LoadContainerList();
        }

        private void containersListView_DoubleClick(object sender, EventArgs e)
        {
            new ContainerViewer(m_server, m_port, containersListView.SelectedItems[0].Text).Show();
        }

        private void containersListView_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.Enter)
            {
                new ContainerViewer(m_server, m_port, containersListView.SelectedItems[0].Text).Show();
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
            new ContainerViewer(m_server, m_port, tbContainer.Text).Show();
        }

        private void Explorer_Load(object sender, EventArgs e)
        {

        }

        private void updateContainerListButton_Click(object sender, EventArgs e)
        {
            LoadContainerList();
        }

        private void updateListViewTimer_Tick(object sender, EventArgs e)
        {
            lock (m_toAddToListView)
            {
                if (m_toAddToListView.Keys.Count == 0)
                    return;

                foreach (string key in m_toAddToListView.Keys)
                {
                    containersListView.Items.Add(
                        new ListViewItem(new string[] { key, m_toAddToListView[key] }));
                }

                m_toAddToListView.Clear();
            }

            statusLabel.Text = String.Format("{0}/{1} containers", containersListView.Items.Count, m_containerCount);
        }

    }
}
