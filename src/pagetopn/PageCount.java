package pagetopn;

public class PageCount implements Comparable<PageCount>{
	private String page;
	private int count;
	
	public void set(String page,int count) {
		this.page=page;
		this.count=count;
	}
	
	/**
	 * @return the page
	 */
	public String getPage() {
		return page;
	}
	/**
	 * @param page the page to set
	 */
	public void setPage(String page) {
		this.page = page;
	}
	/**
	 * @return the count
	 */
	public int getCount() {
		return count;
	}
	/**
	 * @param count the count to set
	 */
	public void setCount(int count) {
		this.count = count;
	}

	@Override
	public int compareTo(PageCount o) {
		// TODO Auto-generated method stub
		return (o.getCount()-this.count)==0?this.page.compareTo(o.getPage()):
			o.getCount()-this.count;
	}

	
	
	
}
