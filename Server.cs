using System;
using System.IO;
using UnityEngine;
using UnityEngine.Assertions;
using System.Collections;
using System.Collections.Generic;
using TheNextMoba.Utils;
using ProtoBuf;
using Apollo;

namespace TheNextMoba.Network
{
	public delegate void ConnectHandle(ConnectEventType type, ApolloResult result);
	public delegate void NetworkMessageHandle(object message);

	public enum ProtocolType:int
	{
		TCP = 0,UDP
	}

	public enum ConnectEventType:int
	{
		Connect = 0, Send, Receive, Error, Reconnect, Disconnect
	}

	public class MessageObject
	{
		public ushort command;
		public object message;
	}

	public class ProtocolPackage
	{
		public const uint HEAD_LENGTH = 24;

		public UInt32 length;	// 4B
		public UInt32 uin;		// 4B
		public UInt16 version;	// 2B
		public UInt32 appID;	// 4B
		public UInt16 zoneID;	// 2B
		public UInt16 command;	// 2B
		public UInt16 checksum;	// 2B
		public UInt32 index;	// 4B

		public byte[] message;

		public byte[] EncodePackage(byte[] message)
		{
			length = HEAD_LENGTH + (uint)message.Length;
			byte[] result = new byte[length];
			uint position = 0;

			EncodeUInt32 (length	, result, ref position);
			EncodeUInt32 (uin		, result, ref position);
			EncodeUInt16 (version	, result, ref position);
			EncodeUInt32 (appID		, result, ref position);
			EncodeUInt16 (zoneID	, result, ref position);
			EncodeUInt16 (command	, result, ref position);
			EncodeUInt16 (checksum	, result, ref position);
			EncodeUInt32 (index		, result, ref position);

			Assert.AreEqual (HEAD_LENGTH, (uint)result.Length);
			Assert.AreEqual (HEAD_LENGTH, position);

			Array.Copy (message, 0, result, position, message.Length);
			return result;
		}

		private void ReadPackageHead(byte[] data)
		{
			using (MemoryStream stream = new MemoryStream (data))
			using (BinaryReader reader = new BinaryReader (stream)) 
			{
				length		= DecodeUInt32 (reader);
				uin			= DecodeUInt32 (reader);
				version		= DecodeUInt16 (reader);
				appID		= DecodeUInt32 (reader);
				zoneID		= DecodeUInt16 (reader);
				command		= DecodeUInt16 (reader);
				checksum	= DecodeUInt16 (reader);
				index 		= DecodeUInt32 (reader);
			}
		}

		private byte[] _buffer;
		private byte[] _remain;
		private uint _bytesReceived;
		private bool _headComplete;
		private bool _bodyComplete;

		public bool ReadConnectionStream(byte[] data)
		{
			if (_buffer == null)
			{
				_buffer = new byte[HEAD_LENGTH];
			}

			uint position = _bytesReceived;

			_bytesReceived += (uint)data.Length;

			byte[] bytes = null;
			if (!_headComplete)
			{
				if (_bytesReceived >= HEAD_LENGTH) 
				{
					bytes = new byte[_bytesReceived];
					Array.Copy (_buffer, 0, bytes, 0, _buffer.Length);
					Array.Copy (data, 0, bytes, _buffer.Length, data.Length);

					ReadPackageHead (bytes);
					_headComplete = true;

					byte[] buffer = new byte[length];
					Array.Copy (bytes, 0, buffer, 0, Math.Min (_bytesReceived, length));

					_buffer = buffer;
				} 
				else 
				{
					Array.Copy (data, 0, _buffer, position, data.Length);
				}
			}

			if (_headComplete)
			{
				if (_bytesReceived >= length) 
				{
					_bodyComplete = true;

					message = new byte[length - HEAD_LENGTH];
					Array.Copy (_buffer, HEAD_LENGTH, message, 0, message.Length);

					if (_bytesReceived > length)
					{
						_remain = new byte[_bytesReceived - length];
						Array.Copy (bytes, length, _remain, 0, _remain.Length);
					}
				} 
				else
				{
					Array.Copy (data, 0, _buffer, position, data.Length);
				}
			}

			return _headComplete && _bodyComplete;
		}

		public bool HeadComplete{ get { return _headComplete; } }
		public bool BodyComplete{ get { return _bodyComplete; } }

		public byte[] StripRemainBytes()
		{
			if (_headComplete && _bodyComplete) 
			{
				byte[] bytes = new byte[_remain.Length];
				Array.Copy (_remain, bytes, _remain.Length);
				return bytes;
			}

			return null;
		}

		public void Clear()
		{
			if (_remain != null) 
			{
				Array.Clear (_remain, 0, _remain.Length);
				_remain = null;
			}

			if (_buffer != null) 
			{
				Array.Clear (_buffer, 0, _buffer.Length);
				_buffer = null;
			}

			if (message != null) 
			{
				Array.Clear (message, 0, message.Length);
				message = null;
			}

			_bytesReceived = 0;
			_headComplete = false;
			_bodyComplete = false;
		}

		UInt16 DecodeUInt16(BinaryReader reader)
		{
			return (UInt16)System.Net.IPAddress.NetworkToHostOrder (reader.ReadInt16 ());
		}

		UInt32 DecodeUInt32(BinaryReader reader)
		{
			return (UInt32)System.Net.IPAddress.NetworkToHostOrder (reader.ReadInt32 ());
		}

		void EncodeUInt16(UInt16 num, byte[] result, ref uint position)
		{
			UInt16 value = (UInt16)System.Net.IPAddress.HostToNetworkOrder ((Int16)num);
			byte[] bytes = BitConverter.GetBytes (value);

			Array.Copy (bytes, 0, result, position, bytes.Length);
			position += (uint)bytes.Length;
		}

		void EncodeUInt32(UInt32 num, byte[] result, ref uint position)
		{
			UInt32 value = (UInt32)System.Net.IPAddress.HostToNetworkOrder ((Int32)num);
			byte[] bytes = BitConverter.GetBytes (value);

			Array.Copy (bytes, 0, result, position, bytes.Length);
			position += (uint)bytes.Length;
		}
	}

	public class Server:SingletonMono<Server>
	{
		private IApolloConnector _connector;
		private string _dhp = "C0FC17D2ADC0007C512E9B6187823F559595D953C82D3D4F281D5198E86C79DF14FAB1F2A901F909FECB71B147DBD265837A254B204D1B5BC5FD64BF804DCD03";

		private ConnectHandle _connectHandle;
		private ProtocolPackage _protocol;
		private ProtocolType _type;

		private Dictionary<ushort, Type> _commandMap = new Dictionary<ushort, Type>();
		private Dictionary<ushort, NetworkMessageHandle> _messageHandles = new Dictionary<ushort, NetworkMessageHandle>();

		private List<MessageObject> _messages = new List<MessageObject>();

		public uint uin { get; set;}

		public Server ()
		{
			ApolloInfo info = new ApolloInfo (102400);
			IApollo.Instance.Initialize (info);
		}

		//MARK: Manage Command Registers
		public void RegisterCommandType(ushort command, Type type)
		{
			Debug.Log("RegisterCommandType : " + command + " type : " + type);
			if (!_commandMap.ContainsKey (command)) 
			{
				_commandMap.Add (command, type);
			}
			else 
			{
				_commandMap [command] = type;
			}
		}

		public void UnregisterCommandType(ushort command)
		{
			Debug.Log("UnregisterCommandType : " + command);
			if (_commandMap.ContainsKey (command)) 
			{
				_commandMap.Remove (command);
			}
		}

		public Type GetTypeByCommand(ushort command)
		{
			Debug.Log("GetTypeByCommand : " + command);
			if (_commandMap.ContainsKey(command))
			{
				return _commandMap [command];
			}

			return null;
		}

		//MARK: Manage Message Handlers
		public void AddMessageHandle(ushort command, NetworkMessageHandle handle)
		{
			if (!_messageHandles.ContainsKey (command)) 
			{
				_messageHandles.Add (command, handle);
			}
			else 
			{
				_messageHandles [command] += handle;
			}
		}

		public void RemoveMessageHandle(ushort command, NetworkMessageHandle handle)
		{
			if (_messageHandles.ContainsKey (command)) 
			{
				_messageHandles [command] -= handle;
			}
		}

		//MARK: Trigger Message Handlers
		private void TriggerHandlesWithMessage(ushort command, object message)
		{
			lock (_messages)
			{
				MessageObject msg = new MessageObject ();
				msg.command = command;
				msg.message = message;
				_messages.Add(msg);
			}
		}

		public void Update()
		{
			lock (_messages) 
			{
				if (_messages.Count > 0) 
				{
					for (int i = 0; i < _messages.Count; i++) 
					{
						MessageObject msg = _messages [i];

						NetworkMessageHandle handle;
						if (_messageHandles.TryGetValue (msg.command, out handle)) 
						{
							try
							{
								if (handle != null)
								{
									handle(msg.message);
								}
							}
							catch (Exception e)
							{
								Debug.LogWarning("TriggerHandlesWithMessage exception : " + e.Message + "  stackTrace : " + e.StackTrace);
							}
						}
					}

					_messages.Clear ();
				}
			}
		}

		//MARK: Manage Connect Event Listeners
		public void AddConnectHandle(ConnectHandle handle)
		{
			_connectHandle -= handle;
			_connectHandle += handle;
		}

		public void RemoveConnectHandle(ConnectHandle handle)
		{
			_connectHandle -= handle;
		}

		//MARK: Connect Operations
		public void Connect(string ip, int port, ProtocolType type = ProtocolType.UDP, string dhp = null)
		{
			_type = type;

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

			_connector.ConnectEvent += new ConnectEventHandler (ApolloConnectHandle);
			_connector.RecvedDataEvent += new RecvedDataHandler(ApolloRecievedDataEventHandle);
			_connector.ErrorEvent += new ConnectorErrorEventHandler (ApolloErrorHandle);
			_connector.DisconnectEvent += new DisconnectEventHandler(ApolloDisconnectHandle);
			_connector.ReconnectEvent += new ReconnectEventHandler (ApolloReconnectHandle);

			_connector.SetSecurityInfo (ApolloEncryptMethod.Aes, ApolloKeyMaking.RawDH, dhp);
			ApolloResult r = _connector.Connect ();
			Debug.Log (r);
		}

		public void Send<T>(ushort command, T message) where T: ProtoBuf.IExtensible
		{
			if (!Connected) 
			{
				Debug.LogError("Client's not connected!");
				return;
			}
			
			// Setup Protocol Head
			ProtocolPackage protocol = new ProtocolPackage();
			protocol.command = command;
			protocol.uin = uin;

			// Serialize Message
			MemoryStream stream = new MemoryStream ();
			Serializer.Serialize<T> (stream, message);

			byte[] data = protocol.EncodePackage (stream.GetBuffer ());

			ApolloResult result;
			if (_type == ProtocolType.TCP) 
			{
				result = _connector.WriteData (data);
			} 
			else 
			{
				result = _connector.WriteUdpData (data);
			}

			DispatchConnectEvent (ConnectEventType.Send, result);
		}

		public void Reconnect()
		{
			_connector.Reconnect ();
		}

		public void Close()
		{
			if (_connector != null) 
			{
				_connector.ConnectEvent -= new ConnectEventHandler (ApolloConnectHandle);
				_connector.RecvedDataEvent -= new RecvedDataHandler(ApolloRecievedDataEventHandle);
				_connector.ErrorEvent -= new ConnectorErrorEventHandler (ApolloErrorHandle);
				_connector.DisconnectEvent -= new DisconnectEventHandler(ApolloDisconnectHandle);
				_connector.ReconnectEvent -= new ReconnectEventHandler (ApolloReconnectHandle);
				_connector.Disconnect ();
				_connector = null;
			}

			if (_protocol != null) 
			{
				_protocol.Clear ();
				_protocol = null;
			}
		}

		public bool Connected
		{
			get { return _connector != null && _connector.Connected; }
		}

		private void ApolloRecievedDataEventHandle()
		{
			byte[] buffer;

			ApolloResult result;
			if (_type == ProtocolType.TCP) 
			{
				result = _connector.ReadData (out buffer);
			}
			else 
			{
				result = _connector.ReadUdpData (out buffer);
			}

			if (_protocol == null) 
			{
				_protocol = new ProtocolPackage ();
			}

			ReadConnectionStream (buffer);
			DispatchConnectEvent (ConnectEventType.Receive, result);
		}

		private void ReadConnectionStream(byte[] buffer)
		{
			if (_protocol.ReadConnectionStream(buffer))
			{
				byte[] remain = _protocol.StripRemainBytes ();

				// Deserialize Message
				Type type = GetTypeByCommand(_protocol.command);
				if (type != null) 
				{
					Debug.Log("[RSP-BODY]command : " + _protocol.command + "message_length : " + _protocol.message.Length + " type : " + type);
					MemoryStream stream = new MemoryStream(_protocol.message);
					object message = Serializer.NonGeneric.Deserialize (type, stream);
					TriggerHandlesWithMessage (_protocol.command, message);
				}

				_protocol.Clear ();
				if (remain != null)
				{
					ReadConnectionStream (remain);
				}
			}
			else
			if (_protocol.HeadComplete)
			{
				string msg = string.Format ("[RSP-HEAD]command:{1} uin:{2} index:{3} length:{4}", _protocol.command, _protocol.uin, _protocol.index, _protocol.length);
				Debug.Log (msg);
			}
		}

		//MARK: Connection Events Handle
		private void ApolloConnectHandle(ApolloResult result, ApolloLoginInfo loginInfo)
		{
			DispatchConnectEvent (ConnectEventType.Connect, result);
			Debug.Log (loginInfo);
		}

		private void ApolloErrorHandle(ApolloResult result)
		{
			DispatchConnectEvent (ConnectEventType.Error, result);
		}

		private void ApolloDisconnectHandle(ApolloResult result)
		{
			DispatchConnectEvent (ConnectEventType.Disconnect, result);
		}

		private void ApolloReconnectHandle(ApolloResult result)
		{
			DispatchConnectEvent (ConnectEventType.Reconnect, result);
		}

		private void DispatchConnectEvent(ConnectEventType type, ApolloResult result)
		{
			Debug.Log (type + ":" + result);

			if (_connectHandle != null) 
			{
				_connectHandle (type, result);
			}
		}

		void OnDestroy()
		{
			Close ();
		}
	}
}

