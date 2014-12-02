package org.dacell.redis.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class NetHelper
{
  private static Log LOG = LogFactory.getLog(NetHelper.class);
  
  public static void main(String[] args){
	  System.out.println(NetHelper.getLocalNetIp().toString());
	  List list=NetHelper.getLocalIpList();
	  for(int i=0;i<list.size();i++){
	  System.out.println(list.get(i).toString());
	  }
  }

  public static InetAddress getLocalNetIp()
  {
    InetAddress localip = null;
    InetAddress netip = null;
    try {
      Enumeration netInterfaces = NetworkInterface.getNetworkInterfaces();
      InetAddress ip = null;
      boolean finded = false;
      do {
        NetworkInterface ni = 
          (NetworkInterface)netInterfaces
          .nextElement();
        Enumeration address = ni.getInetAddresses();
        while (address.hasMoreElements()) {
          ip = (InetAddress)address.nextElement();
          LOG.debug(ni.getName() + ";" + ip.getHostAddress() + 
            ";ip.isSiteLocalAddress()=" + 
            ip.isSiteLocalAddress() + 
            ";ip.isLoopbackAddress()=" + 
            ip.isLoopbackAddress());
          if ((!ip.isSiteLocalAddress()) && (!ip.isLoopbackAddress()) && 
            (ip.getHostAddress().indexOf(":") == -1)) {
            netip = ip;
            finded = true;
            break;
          }if ((!ip.isSiteLocalAddress()) || 
            (ip.isLoopbackAddress()) || 
            (ip.getHostAddress().indexOf(":") != -1)) continue;
          localip = ip;
        }
        if (!netInterfaces.hasMoreElements()) break; 
      }while (!finded);
    }
    catch (SocketException e)
    {
      LOG.error(e);
      throw new RuntimeException(e);
    }
    if (netip != null) {
      return netip;
    }
    return localip;
  }

  public static List getLocalIpList()
  {
    List list = new ArrayList();
    InetAddress localip = null;
    InetAddress netip = null;
    try {
      Enumeration netInterfaces = NetworkInterface.getNetworkInterfaces();
      InetAddress ip = null;
      boolean finded = false;
      do {
        NetworkInterface ni = 
          (NetworkInterface)netInterfaces
          .nextElement();
        Enumeration address = ni.getInetAddresses();
        while (address.hasMoreElements()) {
          ip = (InetAddress)address.nextElement();
          LOG.debug(ni.getName() + ";" + ip.getHostAddress() + 
            ";ip.isSiteLocalAddress()=" + 
            ip.isSiteLocalAddress() + 
            ";ip.isLoopbackAddress()=" + 
            ip.isLoopbackAddress());
          if ((!ip.isSiteLocalAddress()) && (!ip.isLoopbackAddress()) && 
            (ip.getHostAddress().indexOf(":") == -1)) {
            netip = ip;
            finded = true;
            break;
          }if ((!ip.isSiteLocalAddress()) || 
            (ip.isLoopbackAddress()) || 
            (ip.getHostAddress().indexOf(":") != -1)) continue;
          localip = ip;
        }
        if (!netInterfaces.hasMoreElements()) break; 
      }while (!finded);
    }
    catch (SocketException e)
    {
      LOG.error(e);
      throw new RuntimeException(e);
    }
    if (netip != null) {
      list.add(netip.getHostAddress());
    }
    if (localip != null) {
      list.add(localip.getHostAddress());
    }
    return list;
  }
}
