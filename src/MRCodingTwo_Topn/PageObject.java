package MRCodingTwo_Topn;

import java.util.Set;

public class PageObject implements Comparable<PageObject>{

	/**
	 * @return the url
	 */
	public String getUrl() {
		return url;
	}
	/**
	 * @param url the url to set
	 */
	public void setUrl(String url) {
		this.url = url;
	}
	/**
	 * @return the counts
	 */
	public int getCounts() {
		return counts;
	}
	/**
	 * @param counts the counts to set
	 */
	public void setCounts(int counts) {
		this.counts = counts;
	}
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return "PageObject [url=" + url + ", counts=" + counts + "]";
	}
	private String url;
	private int counts;
	
	public void set(String url,int counts) {
		this.url=url;
		this.counts=counts;
	}
	
	@Override
	public int compareTo(PageObject o) {
		// TODO Auto-generated method stub
		return (o.getCounts()-this.counts)==0?
			this.getUrl().compareTo(o.getUrl()):o.getCounts()-this.getCounts();
	}
	
	
	
}
