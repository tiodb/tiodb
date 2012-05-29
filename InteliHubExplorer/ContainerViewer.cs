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

        public ContainerViewer(string server, Int16 port, string containerName)
        {
            m_connection = new InteliHubClient.Connection(server, port);
            m_container = m_connection.Open(containerName);
            
            InitializeComponent();

            Text = m_container.Name;
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

                itemsListView.Items.Clear();

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
            UpdateData();
        }
    }
}
