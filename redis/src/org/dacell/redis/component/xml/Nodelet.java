package org.dacell.redis.component.xml;

import org.w3c.dom.Node;

public abstract interface Nodelet
{
  public abstract void process(Node paramNode)
    throws Exception;
}
