using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;

namespace InteliHubExplorer
{
    public partial class ContainerViewer : Form
    {
        InteliHubClient.Connection m_connection = null;
        InteliHubClient.Container m_container = null;
        string m_server;
        Int16 m_port;
        string m_containerName;

        public ContainerViewer(string server, Int16 port, string containerName)
        {
            m_server = server;
            m_port = port;
            m_containerName = containerName;
            
            InitializeComponent();
        }

        private bool m_updating = false;

        void UpdateData()
        {
            if (m_updating)
            {
                return;
            }

            try
            {
                m_updating = true;

                itemsListView.Items.Clear();

                //
                // let's check if there is a schema for the value
                // 
                string schema = null;
                try
                {
                    schema = m_container.GetProperty("schema");
                }
                catch(Exception){}

                if(!String.IsNullOrEmpty(schema))
                {

                }


                int count = 0;

                m_container.Query(
                    delegate(object key, object value, object metadata)
                    {
                        itemsListView.Items.Add(
                            new ListViewItem(
                                new string[] { Convert.ToString(key), Convert.ToString(value), Convert.ToString(metadata) }));

                        count++;

                        if (count % 1000 == 0)
                        {
                            Application.DoEvents();
                            statusLabel.Text = String.Format("{0} records", count);
                        }
                    });

                statusLabel.Text = String.Format("{0} records", count);
            }
            finally
            {
                m_updating = false;
            }
        }

        private void ContainerViewer_KeyUp(object sender, KeyEventArgs e)
        {
            if (e.KeyCode == Keys.F5)
            {
                UpdateData();
            }
        }

        private void ContainerViewer_Load(object sender, EventArgs e)
        {
            Text = m_containerName;
            containerNameTextBox.Text = m_containerName;

            Application.DoEvents();

            try
            {
                m_connection = new InteliHubClient.Connection(m_server, m_port);
                m_container = m_connection.Open(m_containerName);
            }
            catch (Exception ex)
            {
                MessageBox.Show("ERROR: " + ex.Message);
                Close();
                return;
            }

            UpdateData();
        }
    }
}
