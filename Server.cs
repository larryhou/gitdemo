using System;
using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using TheNextMoba.Utils;
using moba_protocol;
using Apollo;

namespace TheNextMoba.Network
{
	enum ProtocolType:int
	{
		TCP = 0,UDP
	}

	public class Server:SingletonMono<Server>
	{
		private IApolloConnector _connector;
		private string _dhp = "C0FC17D2ADC0007C512E9B6187823F559595D953C82D3D4F281D5198E86C79DF14FAB1F2A901F909FECB71B147DBD265837A254B204D1B5BC5FD64BF804DCD03";

		public Server ()
		{
			ApolloInfo info = new ApolloInfo (102400);
			IApollo.Instance.Initialize (info);
		}

		public void ConnectServer(string ip, int port, ProtocolType type = ProtocolType.UDP, string dhp = null)
		{
			if (dhp == null)
				dhp = _dhp;
			
			if (type == ProtocolType.UDP) 
			{
				_connector = IApollo.Instance.CreateApolloConnection (ApolloPlatform.None, "lwip://" + ip + ":" + port);
			} 
			else 
			{
				_connector = IApollo.Instance.CreateApolloConnection (ApolloPlatform.None, "tcp://" + ip + ":" + port);
			}

			_connector.ConnectEvent += (ApolloResult result, ApolloLoginInfo loginInfo) => 
			{
				
			};

			_connector.SetSecurityInfo (ApolloEncryptMethod.Aes, ApolloKeyMaking.RawDH, dhp);
			ApolloResult result = _connector.Connect ();
			Debug.Log (result);
		}
	}
}

