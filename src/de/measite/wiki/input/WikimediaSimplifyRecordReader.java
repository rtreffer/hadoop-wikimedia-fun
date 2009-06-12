package de.measite.wiki.input;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.DOMImplementation;
import org.w3c.dom.Document;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.sun.org.apache.xml.internal.utils.DOMBuilder;

public class WikimediaSimplifyRecordReader extends MatchRecordReader {

	private SAXTransformerFactory transformFactory;
	private Thread transThread;

	public WikimediaSimplifyRecordReader() {
		transformFactory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
	}

	@Override
	public void endRecord() {
		try {
			out.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		super.endRecord();
		try {
			transThread.join();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void startRecord() throws IOException {
		if (out != null) {
			try {
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
			out = null;
		}
		super.startRecord();
		final PipedOutputStream pout = new PipedOutputStream();
		final CyclicBarrier barrier = new CyclicBarrier(2);
		transThread = new Thread() {
			public void run() {
				final PipedInputStream in = new PipedInputStream(1024*1024);
				try {
					in.connect(pout);
				} catch (IOException e) {
					e.printStackTrace();
				}
				try {
					TransformerHandler handler = transformFactory.newTransformerHandler();
					handler.setResult(new StreamResult(valueBuffer));
					try {
						barrier.await();
					} catch (InterruptedException e) {
						throw new RuntimeException(e);
					} catch (BrokenBarrierException e) {
						throw new RuntimeException(e);
					}
					try {
						in.read();
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					try {
						barrier.await();
					} catch (InterruptedException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					} catch (BrokenBarrierException e) {
						e.printStackTrace();
						throw new RuntimeException(e);
					}
					transformFactory.newTransformer().transform(new SAXSource(new InputSource(new OmitCloseInputStream(in))), new SAXResult(new XMLSwitch(handler)));
				} catch (TransformerConfigurationException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				} catch (TransformerException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				} catch (ParserConfigurationException e) {
					e.printStackTrace();
					throw new RuntimeException(e);
				} finally {
					barrier.reset();
				}
				try {
					while (in.read() != -1) ;
				} catch (IOException e) {
					// not needed
				}
			}
		};
		transThread.start();
		try {
			barrier.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		out = pout;
		try {
			out.write(0);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		try {
			barrier.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		} catch (BrokenBarrierException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	private static class OmitDocumentHandler implements ContentHandler {

		private final ContentHandler delegate;

		public OmitDocumentHandler(ContentHandler delegate) {
			this.delegate = delegate;
		}

		@Override
		public void characters(char[] ch, int start, int length)
		throws SAXException {
			delegate.characters(ch, start, length);
		}

		@Override
		public void endElement(String uri, String localName, String qName)
		throws SAXException {
			delegate.endElement(uri, localName, qName);
		}

		@Override
		public void endPrefixMapping(String prefix) throws SAXException {
			delegate.endPrefixMapping(prefix);
		}

		@Override
		public void ignorableWhitespace(char[] ch, int start, int length)
		throws SAXException {
			delegate.ignorableWhitespace(ch, start, length);
		}

		@Override
		public void processingInstruction(String target, String data)
		throws SAXException {
			delegate.processingInstruction(target, data);
		}

		@Override
		public void setDocumentLocator(Locator locator) {
			delegate.setDocumentLocator(locator);
		}

		@Override
		public void skippedEntity(String name) throws SAXException {
			delegate.skippedEntity(name);
		}

		@Override
		public void startElement(String uri, String localName, String qName,
		Attributes atts) throws SAXException {
			delegate.startElement(uri, localName, qName, atts);
		}

		@Override
		public void startPrefixMapping(String prefix, String uri)
		throws SAXException {
			delegate.startPrefixMapping(prefix, uri);
		}

		@Override
		public void endDocument() throws SAXException {
		}

		@Override
		public void startDocument() throws SAXException {
		}

	}

	private class XMLSwitch extends DefaultHandler {
		private final ContentHandler receiver;
		private ContentHandler ireceiver;
		private Document textDocument;
		private DOMImplementation dom;

		public XMLSwitch(ContentHandler receiver) throws ParserConfigurationException {
			this.receiver = receiver;
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();
			dom = builder.getDOMImplementation();
		}

		@Override
		public void characters(char[] ch, int start, int length)
		throws SAXException {
			ireceiver.characters(ch, start, length);
		}

		@Override
		public void endDocument() throws SAXException {
			ireceiver.endDocument();
		}

		@Override
		public void endElement(String uri, String localName, String qName)
		throws SAXException {
			if (qName.equals("text")) {
				ireceiver.endElement(uri, localName, qName);
				ireceiver.endDocument();
				ireceiver = receiver;
				return;
			}
			if (qName.equals("page")) {
				// Re-emit text
				try {
					transformFactory.newTransformer().transform(new DOMSource(textDocument.getDocumentElement()), new SAXResult(new OmitDocumentHandler(receiver)));
				} catch (TransformerConfigurationException e) {
					e.printStackTrace();
				} catch (TransformerException e) {
					e.printStackTrace();
				}
				receiver.endDocument();
				return;
			}
			ireceiver.endElement(uri, localName, qName);
		}

		@Override
		public void endPrefixMapping(String prefix) throws SAXException {
			ireceiver.endPrefixMapping(prefix);
		}

		@Override
		public void startDocument() throws SAXException {
			ireceiver = receiver;
			ireceiver.startDocument();
		}

		@Override
		public void startElement(String uri, String localName, String qName,
		Attributes attributes) throws SAXException {
			if (qName.equals("text")) {
				textDocument = dom.createDocument(null, null, null);
				DOMBuilder dombuilder = new DOMBuilder(textDocument);
				dombuilder.startDocument();
				ireceiver = dombuilder;
			}
			ireceiver.startElement(uri, localName, qName, attributes);
		}

		@Override
		public void startPrefixMapping(String prefix, String uri)
		throws SAXException {
			ireceiver.startPrefixMapping(prefix, uri);
		}

	}

	private static class OmitCloseInputStream extends FilterInputStream {

		public OmitCloseInputStream(InputStream in) {
			super(in);
		}

		@Override
		public void close() throws IOException {
		}

	}
}
